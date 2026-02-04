"""Integration tests for advanced query patterns (requires datastore emulator)."""
from sqlalchemy import text

# ---------------------------------------------------------------------------
# UPDATE / DELETE via raw SQL
# ---------------------------------------------------------------------------

def test_update_user(conn, datastore_client):
    """Test INSERT then UPDATE of a user via raw SQL."""
    # Insert a test user
    conn.execute(
        text("INSERT INTO users (name, age) VALUES (:name, :age)"),
        {"name": "UpdateTestUser", "age": 20},
    )

    # Find the inserted entity by querying the datastore client directly
    query = datastore_client.query(kind="users")
    query.add_filter("name", "=", "UpdateTestUser")
    entities = list(query.fetch())
    assert len(entities) >= 1
    entity_id = entities[0].key.id

    # Update the user via raw SQL
    conn.execute(
        text("UPDATE users SET age = :age WHERE id = :id"),
        {"age": 25, "id": entity_id},
    )

    # Verify the update
    updated = datastore_client.get(datastore_client.key("users", entity_id))
    assert updated["age"] == 25

    # Cleanup
    datastore_client.delete(datastore_client.key("users", entity_id))


def test_delete_user(conn, datastore_client):
    """Test INSERT then DELETE of a user via raw SQL."""
    # Insert a test user
    conn.execute(
        text("INSERT INTO users (name, age) VALUES (:name, :age)"),
        {"name": "DeleteTestUser", "age": 99},
    )

    # Find the inserted entity
    query = datastore_client.query(kind="users")
    query.add_filter("name", "=", "DeleteTestUser")
    entities = list(query.fetch())
    assert len(entities) >= 1
    entity_id = entities[0].key.id

    # Delete via raw SQL
    conn.execute(
        text("DELETE FROM users WHERE id = :id"),
        {"id": entity_id},
    )

    # Verify deletion
    deleted = datastore_client.get(datastore_client.key("users", entity_id))
    assert deleted is None


# ---------------------------------------------------------------------------
# fetchone / fetchmany
# ---------------------------------------------------------------------------

def test_fetchone(conn):
    """Test fetching a single row at a time."""
    result = conn.execute(text("SELECT * FROM users"))
    row = result.fetchone()
    assert row is not None
    # Fetch remaining
    remaining = result.fetchall()
    assert len(remaining) == 2


def test_fetchmany(conn):
    """Test fetching a batch of rows."""
    result = conn.execute(text("SELECT * FROM users"))
    batch = result.fetchmany(2)
    assert len(batch) == 2
    remaining = result.fetchall()
    assert len(remaining) == 1


# ---------------------------------------------------------------------------
# OR condition queries (client-side filtering)
# ---------------------------------------------------------------------------

def test_or_condition_query(conn):
    """Test OR conditions which require client-side filtering."""
    result = conn.execute(
        text("SELECT * FROM users WHERE country = 'Arland' OR country = 'Britannia'")
    )
    data = result.fetchall()
    assert len(data) == 2


def test_or_with_and_condition(conn):
    """Test compound OR + AND conditions."""
    result = conn.execute(
        text(
            "SELECT * FROM users WHERE (age > 20 AND country = 'Los Santos, San Andreas') "
            "OR country = 'Arland'"
        )
    )
    data = result.fetchall()
    assert len(data) == 2


# ---------------------------------------------------------------------------
# NOT IN queries
# ---------------------------------------------------------------------------

def test_not_in_query(conn):
    """Test NOT IN condition."""
    result = conn.execute(
        text("SELECT * FROM users WHERE country NOT IN ('Arland', 'Britannia')")
    )
    data = result.fetchall()
    assert len(data) == 1


# ---------------------------------------------------------------------------
# Inequality operators
# ---------------------------------------------------------------------------

def test_less_than_query(conn):
    result = conn.execute(text("SELECT * FROM users WHERE age < 20"))
    data = result.fetchall()
    assert len(data) == 2  # age 16 and 14


def test_less_than_equal_query(conn):
    result = conn.execute(text("SELECT * FROM users WHERE age <= 16"))
    data = result.fetchall()
    assert len(data) == 2  # age 16 and 14


def test_greater_than_equal_query(conn):
    result = conn.execute(text("SELECT * FROM users WHERE age >= 16"))
    data = result.fetchall()
    assert len(data) == 2  # age 16 and 28


def test_not_equal_query(conn):
    result = conn.execute(text("SELECT * FROM users WHERE country != 'Arland'"))
    data = result.fetchall()
    assert len(data) == 2


# ---------------------------------------------------------------------------
# IN query
# ---------------------------------------------------------------------------

def test_in_query(conn):
    result = conn.execute(
        text("SELECT * FROM users WHERE country IN ('Arland', 'Britannia')")
    )
    data = result.fetchall()
    assert len(data) == 2


# ---------------------------------------------------------------------------
# Aggregation: SUM and AVG
# ---------------------------------------------------------------------------

def test_aggregate_sum(conn):
    result = conn.execute(
        text("AGGREGATE SUM(reward) OVER (SELECT * FROM tasks)")
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] > 0


def test_aggregate_avg(conn):
    result = conn.execute(
        text("AGGREGATE AVG(reward) OVER (SELECT * FROM tasks)")
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] > 0


def test_aggregate_multiple(conn):
    result = conn.execute(
        text("AGGREGATE COUNT(*), SUM(reward) OVER (SELECT * FROM tasks)")
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] == 3  # COUNT
    assert data[0][1] > 0  # SUM


def test_aggregate_with_where(conn):
    result = conn.execute(
        text(
            "AGGREGATE COUNT(*) OVER (SELECT * FROM tasks WHERE tag = 'House')"
        )
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] == 1


def test_aggregate_count_with_alias(conn):
    result = conn.execute(
        text("AGGREGATE COUNT(*) AS total_tasks OVER (SELECT * FROM tasks)")
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] == 3


# ---------------------------------------------------------------------------
# Derived table queries (additional patterns)
# ---------------------------------------------------------------------------

def test_derived_table_count_all(conn):
    """Test COUNT(*) over derived table."""
    result = conn.execute(
        text(
            """
            SELECT
                COUNT(*) AS cnt
            FROM (SELECT * FROM users) AS virtual_table
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] == 3


def test_derived_table_with_where(conn):
    """Test derived table query with WHERE in subquery."""
    result = conn.execute(
        text(
            """
            SELECT
                name AS name,
                age AS age
            FROM (SELECT * FROM users) AS virtual_table
            LIMIT 2
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 2


def test_derived_table_sum_aggregation(conn):
    result = conn.execute(
        text(
            """
            SELECT
                SUM(reward) AS total_reward
            FROM (SELECT * FROM tasks) AS virtual_table
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] > 0


def test_derived_table_avg_aggregation(conn):
    result = conn.execute(
        text(
            """
            SELECT
                AVG(reward) AS avg_reward
            FROM (SELECT * FROM tasks) AS virtual_table
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 1
    assert data[0][0] > 0


# ---------------------------------------------------------------------------
# ORDER BY with LIMIT/OFFSET
# ---------------------------------------------------------------------------

def test_order_by_desc_with_limit(conn):
    result = conn.execute(
        text("SELECT * FROM users ORDER BY age DESC LIMIT 2")
    )
    data = result.fetchall()
    assert len(data) == 2


def test_order_by_with_offset(conn):
    result = conn.execute(
        text("SELECT * FROM users ORDER BY age ASC LIMIT 10 OFFSET 1")
    )
    data = result.fetchall()
    assert len(data) == 2  # 3 total - 1 offset


# ---------------------------------------------------------------------------
# ORM CRUD with Task model
# ---------------------------------------------------------------------------

def test_task_insert_update_delete_raw_sql(conn, datastore_client):
    """Test INSERT, UPDATE, DELETE on tasks via raw SQL."""
    # Insert a task
    conn.execute(
        text("INSERT INTO tasks (task, tag, reward) VALUES (:task, :tag, :reward)"),
        {"task": "Coverage Test Task", "tag": "TestTag", "reward": 42.5},
    )

    # Find the inserted entity
    query = datastore_client.query(kind="tasks")
    query.add_filter("task", "=", "Coverage Test Task")
    entities = list(query.fetch())
    assert len(entities) >= 1
    entity_id = entities[0].key.id

    # Update the task
    conn.execute(
        text("UPDATE tasks SET reward = :reward WHERE id = :id"),
        {"reward": 99.9, "id": entity_id},
    )
    updated = datastore_client.get(datastore_client.key("tasks", entity_id))
    assert updated["reward"] == 99.9

    # Delete the task
    conn.execute(
        text("DELETE FROM tasks WHERE id = :id"),
        {"id": entity_id},
    )
    deleted = datastore_client.get(datastore_client.key("tasks", entity_id))
    assert deleted is None


# ---------------------------------------------------------------------------
# Parameter substitution tests (via execute with params)
# ---------------------------------------------------------------------------

def test_query_with_string_param(conn):
    result = conn.execute(
        text("SELECT * FROM users WHERE name = :name"),
        {"name": "Elmerulia Frixell"},
    )
    data = result.fetchall()
    assert len(data) == 1


def test_query_with_int_param(conn):
    result = conn.execute(
        text("SELECT * FROM users WHERE age = :age"),
        {"age": 28},
    )
    data = result.fetchall()
    assert len(data) == 1


# ---------------------------------------------------------------------------
# SELECT with tasks table (various data types)
# ---------------------------------------------------------------------------

def test_select_tasks_all(conn):
    result = conn.execute(text("SELECT * FROM tasks"))
    data = result.fetchall()
    assert len(data) == 3


def test_select_tasks_with_boolean_filter(conn):
    result = conn.execute(
        text("SELECT * FROM tasks WHERE is_done = false")
    )
    data = result.fetchall()
    assert len(data) == 3


def test_select_task_specific_columns(conn):
    result = conn.execute(text("SELECT task, reward FROM tasks"))
    data = result.fetchall()
    assert len(data) == 3
    for row in data:
        assert len(row) >= 2
