import datastore_dbapi

# Connect to SQLite (in-memory database for this example)
conn = datastore_dbapi.connect("datastore://project_id=test-api-2")

# Create a cursor object
cursor = conn.cursor()

# Create a table
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        age INTEGER
    )
""")

# Insert data (using parameterized query to prevent SQL injection)
cursor.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))
cursor.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Bob", 25))

# Commit the transaction
conn.commit()

# Query the database
cursor.execute("SELECT id, name, age FROM users")
rows = cursor.fetchall()

# Process results
for row in rows:
    print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")

# Clean up
cursor.close()
conn.close()
