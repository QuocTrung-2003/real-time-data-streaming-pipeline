import psycopg2

conn = psycopg2.connect(
    host="postgres",
    dbname="db",
    user="user",
    password="pass"
)

cursor = conn.cursor()

with open("schema.sql", "r") as f:
    cursor.execute(f.read())

conn.commit()
cursor.close()
conn.close()

print("Database initialized!")