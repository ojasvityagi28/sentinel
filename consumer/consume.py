import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
from scoring.rules import should_flag_geo, is_geo_impossible, is_amount_anomalous, is_velocity_anomalous, initial_geo_baseline
from collections import deque
from datetime import datetime

CACHE = {}   # user_id -> {"timestamps": deque, "amounts": deque, "geo_baseline": (location, datetime) or None}
HISTORY_SIZE = 50
assert HISTORY_SIZE > 10, "HISTORY_SIZE must be larger than MAX_TXNS in is_velocity_anomalous"

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
dbname = os.getenv("POSTGRES_DB")

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"), port=5432,          # use 5433 if you changed the mapping
    user=user, password=password, dbname=dbname,
)
cur = conn.cursor()

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    auto_offset_reset='earliest',
    group_id='sentinel-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)


def get_history(user_id):
    cur.execute(
        "SELECT amount_cents, location, timestamp FROM transactions "
        "WHERE user_id = %s ORDER BY timestamp DESC LIMIT %s",
        (user_id, HISTORY_SIZE),
    )
    return cur.fetchall()      # newest first: row[0]=amount, row[1]=location, row[2]=timestamp

def get_geo_baseline(user_id):
    cur.execute(
        "SELECT location, timestamp FROM transactions t "
        "WHERE t.user_id = %s "
        "AND NOT EXISTS (SELECT 1 FROM alerts a "
        "                WHERE a.transaction_id = t.transaction_id AND a.rule = 'geo') "
        "ORDER BY t.timestamp DESC LIMIT 1",
        (user_id,),
    )
    return cur.fetchone()  

def get_amount_history(user_id):
    cur.execute(
        "SELECT amount_cents FROM transactions t "
        "WHERE t.user_id = %s "
        "AND NOT EXISTS (SELECT 1 FROM alerts a "
        "                WHERE a.transaction_id = t.transaction_id "
        "                AND a.rule = 'amount' AND a.status = 'open') "
        "ORDER BY t.timestamp DESC LIMIT %s",
        (user_id, HISTORY_SIZE),
    )
    return [row[0] for row in cur.fetchall()]

def get_home_city(user_id):
    cur.execute("SELECT home_city FROM users WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    return row[0] if row else None

def load_user(user_id, first_txn_time):
    rows = get_history(user_id)
    timestamps = deque(maxlen=HISTORY_SIZE)
    for row in reversed(rows):
        timestamps.append(row[2])

    amounts = deque(maxlen=HISTORY_SIZE)
    for a in reversed(get_amount_history(user_id)):
        amounts.append(a)

    CACHE[user_id] = {
        "timestamps": timestamps,
        "amounts": amounts,
        "geo_baseline": initial_geo_baseline(
            get_geo_baseline(user_id),      # newest transaction without a geo alert, or None
            get_home_city(user_id),         # registered home, or None
            first_txn_time,                 # the time to seed home with
        )
    }

def score_cached(txn, entry):
    fired = []

    trusted = None
    if entry["geo_baseline"]:
        trusted = {"location": entry["geo_baseline"][0], "timestamp": entry["geo_baseline"][1].isoformat()}

    if should_flag_geo(trusted, txn):
        fired.append("geo")

    if is_amount_anomalous(txn["amount_cents"], entry["amounts"]):
        fired.append("amount")

    past_timestamps = [t.isoformat() for t in entry["timestamps"]]
    if is_velocity_anomalous(txn["timestamp"], past_timestamps):
        fired.append("velocity")

    return fired


for message in consumer:
    txn = message.value

    user_id = txn["user_id"]
    if user_id not in CACHE:
        load_user(user_id, datetime.fromisoformat(txn["timestamp"]))            # cold start: runs the three queries once

    entry = CACHE[user_id]
    fired = score_cached(txn, entry)
    
    cur.execute(                               # insert, ignoring duplicates
        "INSERT INTO transactions "
        "(transaction_id, user_id, amount_cents, merchant, location, timestamp) "
        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING",
        (txn["transaction_id"], txn["user_id"], txn["amount_cents"],
         txn["merchant"], txn["location"], txn["timestamp"]),
    )
    is_new = cur.rowcount == 1

    if is_new:
        for rule in fired:
            cur.execute(
                "INSERT INTO alerts (transaction_id, rule) VALUES (%s, %s)",
                (txn["transaction_id"], rule),
            )

    conn.commit()

    if is_new:                                 # update the cache only after the commit succeeded
        txn_time = datetime.fromisoformat(txn["timestamp"])
        entry["timestamps"].append(txn_time)
        if "amount" not in fired:
            entry["amounts"].append(txn["amount_cents"])
        if "geo" not in fired:
            entry["geo_baseline"] = (txn["location"], txn_time)
    print(f"user={user_id} partition={message.partition} offset={message.offset} fired={fired}")