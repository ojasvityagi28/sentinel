import json
import psycopg2
from kafka import KafkaConsumer

from scoring.rules import is_geo_impossible, is_amount_anomalous, is_velocity_anomalous

HISTORY_SIZE = 50

conn = psycopg2.connect(
    host="localhost", port=5432,          # use 5433 if you changed the mapping
    user="<REMOVED>", password="<REMOVED>", dbname="<REMOVED>",
)
cur = conn.cursor()

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers='localhost:9092',
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


def score(txn, history,amount_history, geo_baseline):
    fired = []

    if geo_baseline:                               # rule 1: last transaction not flagged by geo
        prev = {"location": geo_baseline[0], "timestamp": geo_baseline[1].isoformat()}
        if is_geo_impossible(prev, txn):
            fired.append("geo")

    if is_amount_anomalous(txn["amount_cents"], amount_history):
        fired.append("amount")

    past_timestamps = [row[2].isoformat() for row in history]
    if is_velocity_anomalous(txn["timestamp"], past_timestamps):
        fired.append("velocity")

    return fired


for message in consumer:
    txn = message.value

    history = get_history(txn["user_id"])
    geo_baseline = get_geo_baseline(txn["user_id"])
    amount_history = get_amount_history(txn["user_id"])
    
    fired = score(txn, history, amount_history, geo_baseline)

    cur.execute(                            
        "INSERT INTO transactions "
        "(transaction_id, user_id, amount_cents, merchant, location, timestamp) "
        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING",
        (txn["transaction_id"], txn["user_id"], txn["amount_cents"],
         txn["merchant"], txn["location"], txn["timestamp"]),
    )

    if cur.rowcount == 1:                      # 1 = new row, 0 = duplicate delivery
        for rule in fired:
            cur.execute(
                "INSERT INTO alerts (transaction_id, rule) VALUES (%s, %s)",
                (txn["transaction_id"], rule),
            )

    conn.commit()
    print(f"user={txn['user_id']} partition={message.partition} offset={message.offset} fired={fired}")