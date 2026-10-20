import os
import psycopg2
from dotenv import load_dotenv
from jobs.tasks import review_amount_alerts
load_dotenv()

conn = psycopg2.connect(
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="localhost"
)

dismissed = review_amount_alerts(conn)
print(f"Dismissed {len(dismissed)} alerts: {sorted(dismissed)}")