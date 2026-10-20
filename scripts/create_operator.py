import getpass
import os
import psycopg2
from dotenv import load_dotenv
from api.auth import hash_password

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
dbname = os.getenv("POSTGRES_DB")

email = input("email: ")
operator_password = str(getpass.getpass("password: "))

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"), port=5432,
    user=user, password=password, dbname=dbname,
)
cur = conn.cursor()
cur.execute(
    "INSERT INTO operators (email, hashed_password) VALUES (%s, %s)",
    (email, hash_password(operator_password)),
)
conn.commit()
print("operator created")