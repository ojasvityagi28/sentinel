import os
from datetime import date

import psycopg2
from dotenv import load_dotenv

from jobs.tasks import generate_report

load_dotenv()

conn = psycopg2.connect(
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="localhost",
    port=5432,
)

os.makedirs("reports", exist_ok=True)
path = generate_report(conn, "reports", date.today().isoformat())
print(f"Wrote {path}")