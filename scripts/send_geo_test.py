import json
import uuid
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)

for city, seconds_ago in [("Delhi", 40), ("London", 20), ("London", 0)]:
    txn = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": 99,
        "amount_cents": 5000,
        "merchant": "Test",
        "location": city,
        "timestamp": (now - timedelta(seconds=seconds_ago)).isoformat(),
    }
    print("99:" + json.dumps(txn))