# Copyright (c) 2025 The sqlalchemy-datastore Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from sqlalchemy import create_engine, text
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./test_credentials.json"

# Create test dataset
from google.cloud import datastore
client = datastore.Client(project="python-datastore-sqlalchemy")

# user1 Alice
user1 = datastore.Entity(client.key('users'))
user1["name"] = "Alice"
user1["age"] = 15
user1["city"] = "Taipei"

# user2 Bob
user2 = datastore.Entity(client.key('users'))
user2["name"] = "Bob"
user2["age"] = 25 
user2["city"] = "Taichung"

# user3 Carol
user3 = datastore.Entity(client.key('users'))
user3["name"] = "Carol"
user3["age"] = 30
user3["city"] = "Tainan"

batch = client.batch()
batch.begin()
batch.put(user1)
batch.put(user2)
batch.put(user3)
batch.commit()

# task1 for Alice
task1 = datastore.Entity(client.key('tasks'))
task1["task"] = "Crafting Sea Urchins in Atelier"
task1["content"] = {"title": ""} 
task1["is_done"] = False
task1["tag"] = "house"

# task2 for Bob
task2 = datastore.Entity(client.key('tasks'))
task2["task"] = "Crafting Sea Urchins in Atelier"
task2["content"] = 15
task2["is_done"] = False
task2["tag"] = "Wild"

# task3 for Carol
task3 = datastore.Entity(client.key('tasks'))
task3["task"] = ""
task3["content"] = 15
task3["is_done"] = False
task3["tag"] = "house"

engine = create_engine('datastore://python-datastore-sqlalchemy', echo=True)
conn = engine.connect()

# Query the database
# GQL reference can be found at https://cloud.google.com/datastore/docs/gql_reference
result = conn.execute(text("SELECT * FROM users"))
data = result.all()
assert len(data) == 3, "Expected 3 rows in the users table, but found a different number."

# More Query
result = conn.execute(text("SELECT id, name, age FROM users WHERE age > 20"))
data = result.all()
assert len(data) == 1, "Expected 1 row with age > 20, but found a different number."

# Query with specific conditions
result = conn.execute(text("SELECT id, name, age FROM users WHERE name = 'Alice'"))
data = result.all()
assert len(data) == 1, "Expected 1 row with name 'Alice', but found a different number."

# Query for keys
result = conn.execute(text("SELECT __key__ FROM users"))
data = result.all()
assert len(data) == 3, "Expected 3 keys in the users table, but found a different number."

result = conn.execute(text("SELECT __key__ WHERE __key__ = KEY('users', 'alice_id')"))
data = result.all()
assert len(data) == 1

# Query for specific columns 
result = conn.execute(text("SELECT name, age FROM useers"))
data = result.all()
assert len(data) == 3, "Expected 3 rows in the users table, but found a different number."
for row in data:
    assert row[0] in ['Alice', 'Bob', 'Carol'], "Expected names 'Alice', 'Bob', or 'Carol' in the users table, but found a different name."
    assert row[1] in [30, 24, 35], "Expected ages 30, 24, or 35 in the users table, but found a different age."

# Query for fully-qualified property names
result = conn.execute(text("SELECT users.name, users.age FROM users"))
data = result.all()
assert len(data) == 3, "Expected 3 rows in the users table, but found a different number."
for row in data:
    assert row[0] in ['Alice', 'Bob', 'Carol'], "Expected names 'Alice', 'Bob', or 'Carol' in the users table, but found a different name."
    assert row[1] in [30, 24, 35], "Expected ages 30, 24, or 35 in the users table, but found a different age."

# Clause Query
## Distinct query
result = conn.execute(text("SELECT DISTINCT name FROM users"))
data = result.all()
assert len(data) == 3, "Expected 3 distinct names in the users table, but found a different number."
for row in data:
    assert row[0] in ['Alice', 'Bob', 'Carol'], "Expected names 'Alice', 'Bob', or 'Carol' in the users table, but found a different name." 

result = conn.execute(text("SELECT DISTINCT name, age FROM users WHERE age > 20 ORDER BY age DESC LIMIT 10 OFFSET 5"))
data = result.all()
assert len(data) == 1, "Expected 1 distinct row with age > 20, but found a different number."

## Distinct on query
result = conn.execute(text("SELECT DISTINCT ON (name) name, age FROM users ORDER BY name, age DESC"))
data = result.all()
assert len(data) == 3, "Expected 3 distinct names in the users table, but found a different number."

result = conn.execute(text("SELECT DISTINCT ON (name) name, age FROM users WHERE age > 20 ORDER BY name ASC, age DESC LIMIT 10"))
data = result.all()
assert len(data) == 3, ""

## Order by query
result = conn.execute(text("SELECT * FROM users ORDER BY age ASC LIMIT 5"))
data = result.all()
assert len(data) == 3

## Compound query
result = conn.execute(text("SELECT DISTINCT ON (name, age) name, age, city FROM users WHERE age >= 18 AND city = 'Tokyo' ORDER BY name ASC, age DESC LIMIT 20 OFFSET 10"))
data = result.all()
assert len(data) == 3

# Aggregration query
result = conn.execute(text("AGGREGATE COUNT(*) OVER ( SELECT * FROM tasks WHERE is_done = false AND tag = 'house' )"))
data = result.all()
assert len(data) == 3

result = conn.execute(text("AGGREGATE COUNT_UP_TO(5) OVER ( SELECT * FROM tasks WHERE is_done = false AND tag = 'house' )"))
data = result.all()
assert len(data) == 3


# Insert data (using parameterized query to prevent SQL injection)
result = conn.execute(text("INSERT INTO users (name, age) VALUES ('Bob', 25)"))
print(result)
result = conn.execute(
    text("INSERT INTO users (name, age) VALUES (:name, :age)"),
    {"name": "Alice", "age": 30}
)
print(result)

from src import CloudDatastoreDialect
stmt = text("INSERT INTO users (name, age) VALUES (:name, :age)")
compiled = stmt.compile(dialect=CloudDatastoreDialect())
print(str(compiled))
with engine.connect() as conn:
    conn.execute(
        stmt,
        {"name": "Alice", "age": 30}
    )

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
