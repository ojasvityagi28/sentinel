import os

import psycopg2
from dotenv import load_dotenv

from jobs.tasks import compute_risk_profiles

load_dotenv()

conn = psycopg2.connect(
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="localhost",
    port=5432,
)

rows = compute_risk_profiles(conn)
print(f"Wrote profiles for {len(rows)} users")