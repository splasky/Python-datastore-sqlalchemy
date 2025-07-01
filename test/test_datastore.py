from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./test_credentials.json"

Base = declarative_base()
engine = create_engine('datastore://project_id=test-api-2', echo=True)
conn = engine.connect()

# no cursor in datastore
# cursor = conn.cursor()

# Datastore has no create table command 
# conn.execute(text("""
#     CREATE TABLE users (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         name TEXT,
#         age INTEGER
#     )
# """))

# Insert data (using parameterized query to prevent SQL injection)
conn.execute(
    text("INSERT INTO users (name, age) VALUES (:name, :age)"),
    {"name": "Alice", "age": 30}
)
conn.execute(text("INSERT INTO users (name, age) VALUES ('Bob', 25)"))

# Commit the transaction
conn.commit()

# Query the database
result = conn.execute(text("SELECT id, name, age FROM users"))
rows = result.fetchall()

# Process results
for row in rows:
    print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")

# Clean up
conn.close()
