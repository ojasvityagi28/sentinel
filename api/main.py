from contextlib import asynccontextmanager
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime
from typing import Optional
from uuid import UUID
from fastapi import Query
from pydantic import BaseModel
from api.auth import verify_password, create_access_token, get_current_operator
from fastapi import FastAPI, Depends, Query, HTTPException
from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
dbname = os.getenv("POSTGRES_DB")

pool = None

@asynccontextmanager
async def lifespan(app):
    global pool
    pool = ThreadedConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("POSTGRES_HOST", "localhost"), port=5432,        # 5433 if you changed the mapping
        user=user, password=password, dbname=dbname,
    )
    yield
    pool.closeall()


app = FastAPI(lifespan=lifespan)


def get_conn():
    conn = pool.getconn()          # borrow a connection
    try:
        yield conn
    finally:
        pool.putconn(conn)         # always give it back


@app.get("/health")
def health(conn=Depends(get_conn)):
    cur = conn.cursor()
    cur.execute("SELECT 1")
    return {"status": "ok", "db": cur.fetchone()[0] == 1}

@app.get("/transactions", dependencies=[Depends(get_current_operator)])
def list_transactions(
    user_id: Optional[int] = None,
    before: Optional[datetime] = None,
    before_id: Optional[UUID] = None,
    limit: int = Query(20, ge=1, le=100),
    conn=Depends(get_conn),
):
    if (before is None) != (before_id is None):
        raise HTTPException(status_code=422, detail="before and before_id must be sent together")
    conditions = []
    params = []

    if user_id is not None:
        conditions.append("user_id = %s")
        params.append(user_id)
    if before is not None and before_id is not None:
        conditions.append("(timestamp, transaction_id) < (%s, %s)")
        params.extend([before, str(before_id)])

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    sql = (
        "SELECT transaction_id, user_id, amount_cents, merchant, location, timestamp "
        "FROM transactions " + where + " ORDER BY timestamp DESC, transaction_id DESC LIMIT %s"
    )
    params.append(limit)

    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()

    items = []
    for r in rows:
        items.append({
            "transaction_id": str(r[0]),
            "user_id": r[1],
            "amount_cents": r[2],
            "merchant": r[3],
            "location": r[4],
            "timestamp": r[5].isoformat(),
        })

    next_before = None
    next_before_id = None
    if len(rows) == limit:
        next_before = items[-1]["timestamp"]
        next_before_id = items[-1]["transaction_id"]

    return {"items": items, "next_before": next_before, "next_before_id": next_before_id}

@app.get("/alerts", dependencies=[Depends(get_current_operator)])
def list_alerts(
    user_id: Optional[int] = None,
    rule: Optional[str] = None,
    status: Optional[str] = None,
    before_id: Optional[int] = None,
    limit: int = Query(20, ge=1, le=100),
    conn=Depends(get_conn),
):
    conditions = []
    params = []

    if user_id is not None:
        conditions.append("t.user_id = %s")
        params.append(user_id)
    if rule is not None:
        conditions.append("a.rule = %s")
        params.append(rule)
    if status is not None:
        conditions.append("a.status = %s")
        params.append(status)
    if before_id is not None:
        conditions.append("a.alert_id < %s")
        params.append(before_id)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    sql = (
        "SELECT a.alert_id, a.rule, a.status, a.created_at, "
        "t.transaction_id, t.user_id, t.amount_cents, t.location, t.timestamp "
        "FROM alerts a JOIN transactions t ON t.transaction_id = a.transaction_id "
        + where + " ORDER BY a.alert_id DESC LIMIT %s"
    )
    params.append(limit)

    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()

    items = []
    for r in rows:
        items.append({
            "alert_id": r[0],
            "rule": r[1],
            "status": r[2],
            "created_at": r[3].isoformat(),
            "transaction_id": str(r[4]),
            "user_id": r[5],
            "amount_cents": r[6],
            "location": r[7],
            "timestamp": r[8].isoformat(),
        })

    next_before_id = None
    if len(rows) == limit:
        next_before_id = items[-1]["alert_id"]

    return {"items": items, "next_before_id": next_before_id}

@app.get("/users/{user_id}/risk-profile", dependencies=[Depends(get_current_operator)])
def risk_profile(user_id: int, conn=Depends(get_conn)):
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM transactions WHERE user_id = %s", (user_id,))
    total = cur.fetchone()[0]
    if total == 0:
        raise HTTPException(status_code=404, detail="no transactions for this user")

    cur.execute(
        "SELECT count(DISTINCT a.transaction_id) "
        "FROM alerts a JOIN transactions t ON t.transaction_id = a.transaction_id "
        "WHERE t.user_id = %s AND a.status = 'open'",
        (user_id,),
    )
    flagged = cur.fetchone()[0]

    cur.execute(
        "SELECT a.rule, count(*) "
        "FROM alerts a JOIN transactions t ON t.transaction_id = a.transaction_id "
        "WHERE t.user_id = %s AND a.status = 'open' GROUP BY a.rule",
        (user_id,),
    )
    by_rule = {}
    for rule, n in cur.fetchall():
        by_rule[rule] = n

    cur.execute(
        "SELECT max(a.created_at) "
        "FROM alerts a JOIN transactions t ON t.transaction_id = a.transaction_id "
        "WHERE t.user_id = %s AND a.status = 'open'",
        (user_id,),
    )
    last_alert = cur.fetchone()[0]

    return {
        "user_id": user_id,
        "transaction_count": total,
        "flagged_transactions": flagged,
        "alert_rate": round(flagged / total, 4),
        "open_alerts_by_rule": by_rule,
        "last_alert_at": last_alert.isoformat() if last_alert else None,
    }

class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/auth/login")
def login(body: LoginRequest, conn=Depends(get_conn)):
    cur = conn.cursor()
    cur.execute(
        "SELECT operator_id, hashed_password FROM operators WHERE email = %s",
        (body.email,),
    )
    row = cur.fetchone()
    if not row or not verify_password(body.password, row[1]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_access_token({"operator_id": row[0]})
    return {"access_token": token, "token_type": "bearer"}