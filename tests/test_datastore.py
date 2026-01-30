# Copyright (c) 2025 hychang <hychang.1997.tw@gmail.com>
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
from sqlalchemy import text

from sqlalchemy_datastore import CloudDatastoreDialect


def test_select_all_users(conn):
    result = conn.execute(text("SELECT * FROM users"))
    data = result.fetchall()
    assert (
        len(data) == 3
    ), "Expected 3 rows in the users table, but found a different number."


def test_select_users_with_none_result(conn):
    result = conn.execute(text("SELECT * FROM users where age > 99999999"))
    data = result.all()
    assert len(data) == 0, "Should return empty list"


def test_select_users_age_gt_20(conn):
    result = conn.execute(text("SELECT id, name, age FROM users WHERE age > 20"))
    data = result.fetchall()
    assert (
        len(data) == 1
    ), f"Expected 1 rows with age > 20, but found {len(data)}."


def test_select_user_named(conn):
    result = conn.execute(
        text("SELECT id, name, age FROM users WHERE name = 'Elmerulia Frixell'")
    )
    data = result.fetchall()
    assert (
        len(data) == 1
    ), f"Expected 1 row with name 'Elmerulia Frixell', but found {len(data)}."


def test_select_user_keys(conn):
    result = conn.execute(text("SELECT __key__ FROM users"))
    data = result.fetchall()
    assert (
        len(data) == 3
    ), "Expected 3 keys in the users table, but found a different number."

    result = conn.execute(
        text(
            "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
        )
    )
    data = result.fetchall()
    assert len(data) == 1, "Expected to find one key for 'Elmerulia Frixell_id'"


def test_select_specific_columns(conn):
    result = conn.execute(text("SELECT name, age FROM users"))
    data = result.fetchall()
    assert len(data) == 3, "Expected 3 rows in the users table"
    for name, age in data:
        assert name in [
            "Elmerulia Frixell",
            "Virginia Robertson",
            "Travis 'Ghost' Hayes",
        ], f"Unexpected name: {name}"
        assert age in [16, 14, 28], f"Unexpected age: {age}"


def test_fully_qualified_properties(conn):
    result = conn.execute(text("SELECT users.name, users.age FROM users"))
    data = result.fetchall()
    assert len(data) == 3
    for name, age in data:
        assert name in [
            "Elmerulia Frixell",
            "Virginia Robertson",
            "Travis 'Ghost' Hayes",
        ]
        assert age in [16, 14, 28]


def test_distinct_name_query(conn):
    result = conn.execute(text("SELECT DISTINCT name FROM users"))
    data = result.fetchall()
    assert len(data) == 3
    for (name,) in data:
        assert name in [
            "Elmerulia Frixell",
            "Virginia Robertson",
            "Travis 'Ghost' Hayes",
        ]


def test_distinct_name_age_with_conditions(conn):
    result = conn.execute(
        text(
            "SELECT DISTINCT name, age FROM users WHERE age > 13 ORDER BY age DESC LIMIT 10 OFFSET 2"
        )
    )
    data = result.fetchall()
    assert len(data) == 1, f"Expected 1 row (3 total - 2 offset), got {len(data)}"


def test_distinct_on_query(conn):
    result = conn.execute(
        text("SELECT DISTINCT ON (name) name, age FROM users ORDER BY name, age DESC")
    )
    data = result.fetchall()
    assert len(data) == 3, f"Expected 3 rows with DISTINCT ON, got {len(data)}"
    assert data[0][0] == "Elmerulia Frixell"
    assert data[1][0] == "Travis 'Ghost' Hayes"
    assert data[2][0] == "Virginia Robertson"


def test_order_by_query(conn):
    result = conn.execute(text("SELECT * FROM users ORDER BY age ASC LIMIT 5"))
    data = result.fetchall()
    assert len(data) == 3


def test_compound_query(conn):
    # Test compound query with multiple WHERE conditions (emulator-compatible)
    # Note: ORDER BY on different property than WHERE requires composite index
    result = conn.execute(
        text(
            "SELECT DISTINCT ON (name, age) name, age, country FROM users "
            "WHERE age >= 15 AND country = 'Arland' LIMIT 20"
        )
    )
    data = result.fetchall()
    assert len(data) == 1, f"Expected 1 rows, got {len(data)}"


def test_aggregate_count(conn):
    result = conn.execute(
        text(
            "AGGREGATE COUNT(*) OVER ( SELECT * FROM tasks WHERE is_done = false AND additional_notes IS NULL )"
        )
    )
    data = result.fetchall()
    assert len(data) == 1, "Aggregate should return one row"
    assert data[0][0] == 2, f"Expected count of 2, got {data[0][0]}"


def test_aggregate_count_up_to(conn):
    result = conn.execute(
        text(
            "AGGREGATE COUNT_UP_TO(5) OVER ( SELECT * FROM tasks WHERE is_done = false AND additional_notes IS NULL )"
        )
    )
    data = result.fetchall()
    assert len(data) == 1, "Aggregate should return one row"
    assert data[0][0] == 2, f"Expected count of 2 (capped at 5), got {data[0][0]}"


def test_derived_table_query_count_distinct(conn):
    result = conn.execute(
        text(
            """
            SELECT
                task AS task,
                MAX(reward) AS 'MAX(reward)'
            FROM
                ( SELECT *  FROM tasks) AS virtual_table
            GROUP BY task
            ORDER BY 'MAX(reward)' DESC
            LIMIT 10
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 3


def test_derived_table_query_as_virtual_table(conn):
    result = conn.execute(
        text(
            """
            SELECT
                name AS name,
                age AS age,
                country AS country,
                create_time AS create_time,
                description AS description
            FROM (
                SELECT * FROM users
                ) AS virtual_table
            LIMIT 10
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 3


def test_derived_table_query_with_user_key(conn):
    result = conn.execute(
        text(
            """
            SELECT
                assign_user AS assign_user,
                MAX(reward) AS 'MAX(reward)'
            FROM
                ( SELECT *  FROM tasks) AS virtual_table
            GROUP BY assign_user
            ORDER BY 'MAX(reward)' DESC
            LIMIT 10
            """
        )
    )
    data = result.fetchall()
    assert len(data) == 3


def test_insert_data(conn, datastore_client):
    result = conn.execute(
        text("INSERT INTO users (name, age) VALUES ('Virginia Robertson', 25)")
    )
    assert result.rowcount == 1

    result = conn.execute(
        text("INSERT INTO users (name, age) VALUES (:name, :age)"),
        {"name": "Elmerulia Frixell", "age": 30},
    )
    assert result.rowcount == 1

    # Cleanup: delete any users with numeric IDs (original test data uses named keys)
    query = datastore_client.query(kind="users")
    for user in query.fetch():
        # Original test data uses named keys like "Elmerulia Frixell_id"
        # Inserted entities get auto-generated numeric IDs
        if user.key.id is not None:
            datastore_client.delete(user.key)


def test_insert_with_custom_dialect(engine, datastore_client):
    stmt = text("INSERT INTO users (name, age) VALUES (:name, :age)")
    compiled = stmt.compile(dialect=CloudDatastoreDialect())
    print(str(compiled))  # Optional: only for debug

    with engine.connect() as conn:
        conn.execute(stmt, {"name": "Elmerulia Frixell", "age": 30})
        conn.commit()

    # Cleanup: delete any users with numeric IDs (original test data uses named keys)
    query = datastore_client.query(kind="users")
    for user in query.fetch():
        if user.key.id is not None:
            datastore_client.delete(user.key)


def test_query_and_process(conn):
    result = conn.execute(text("SELECT __key__, name, age FROM users"))
    rows = result.fetchall()
    for row in rows:
        print(f"Key: {row[0]}, Name: {row[1]}, Age: {row[2]}")
