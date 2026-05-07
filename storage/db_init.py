import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = conn.cursor()

with open("schema.sql", "r") as f:
    cursor.execute(f.read())

conn.commit()
cursor.close()
conn.close()

print("Database initialized!")