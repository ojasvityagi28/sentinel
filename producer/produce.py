from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime,timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Connected to Kafka!")

CITIES = ["Delhi", "Mumbai", "Bangalore", "London", "Dubai"]
AWAY_PROB = 0.02      # chance a transaction happens outside the home city
SPIKE_PROB = 0.01     # chance of an unusually large amount
BURST_PROB = 0.005    # chance per loop of a 12-transaction burst


def home_city(user_id):
    return CITIES[user_id % len(CITIES)]


def typical_amount(user_id):
    return 2000 + user_id * 1000          # cents, a different "normal" per user


def generate_transaction(user_id=None):
    if user_id is None:
        user_id = random.randint(1, 20)

    if random.random() < AWAY_PROB:
        others = [c for c in CITIES if c != home_city(user_id)]
        location = random.choice(others)
    else:
        location = home_city(user_id)

    base = typical_amount(user_id)
    amount = int(random.gauss(base, base * 0.1))      # spread = 10% of normal
    amount = max(amount, 100)
    if random.random() < SPIKE_PROB:
        amount = base * 10

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount_cents": amount,
        "merchant": random.choice(["Amazon", "Starbucks", "Uber", "Zara", "Steam"]),
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


while True:
    if random.random() < BURST_PROB:
        burst_user = random.randint(1, 20)
        for _ in range(12):
            txn = generate_transaction(burst_user)
            producer.send("transactions", key=txn["user_id"], value=txn)
            print(f"BURST: {txn}")
            time.sleep(0.2)
    else:
        txn = generate_transaction()
        producer.send("transactions", key=txn["user_id"], value=txn)
        print(f"Sent: {txn}")
        time.sleep(1)