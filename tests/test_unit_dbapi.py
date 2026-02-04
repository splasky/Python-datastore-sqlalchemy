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
"""Unit tests for datastore_dbapi module internals (no emulator required)."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from sqlalchemy_datastore.datastore_dbapi import (
    Column,
    Connection,
    Cursor,
    Error,
    ParseEntity,
    ProgrammingError,
    apilevel,
    connect,
    paramstyle,
    threadsafety,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

def test_module_constants():
    assert apilevel == "2.0"
    assert threadsafety == 2
    assert paramstyle == "named"


# ---------------------------------------------------------------------------
# Connection class
# ---------------------------------------------------------------------------

def test_connection_cursor():
    conn = Connection(client=MagicMock())
    cursor = conn.cursor()
    assert isinstance(cursor, Cursor)


def test_connection_begin_commit_rollback_close():
    conn = Connection(client=MagicMock())
    conn.begin()
    conn.commit()
    conn.rollback()
    conn.close()


def test_connect_function():
    client = MagicMock()
    conn = connect(client=client)
    assert isinstance(conn, Connection)
    assert conn._client is client


def test_connect_without_client():
    conn = connect()
    assert isinstance(conn, Connection)
    assert conn._client is None


# ---------------------------------------------------------------------------
# Column namedtuple
# ---------------------------------------------------------------------------

def test_column_namedtuple():
    col = Column(
        name="test",
        type_code="STRING",
        display_size=None,
        internal_size=None,
        precision=None,
        scale=None,
        null_ok=True,
    )
    assert col.name == "test"
    assert col.type_code == "STRING"
    assert col.null_ok is True


# ---------------------------------------------------------------------------
# Cursor – basic lifecycle
# ---------------------------------------------------------------------------

def _make_cursor():
    client = MagicMock()
    conn = Connection(client=client)
    return Cursor(conn)


def test_cursor_initial_state():
    cursor = _make_cursor()
    assert cursor.rowcount == -1
    assert cursor.arraysize is None
    assert cursor._closed is False
    assert cursor.description is None
    assert cursor.lastrowid is None


def test_cursor_close():
    cursor = _make_cursor()
    cursor.close()
    assert cursor._closed is True
    assert cursor.connection is None


def test_cursor_execute_after_close():
    cursor = _make_cursor()
    cursor.close()
    with pytest.raises(Error, match="Cursor is closed"):
        cursor.execute("SELECT * FROM users")


def test_cursor_fetchall_after_close():
    cursor = _make_cursor()
    cursor.close()
    with pytest.raises(Error, match="Cursor is closed"):
        cursor.fetchall()


def test_cursor_fetchone_after_close():
    cursor = _make_cursor()
    cursor.close()
    with pytest.raises(Error, match="Cursor is closed"):
        cursor.fetchone()


def test_cursor_fetchmany_after_close():
    cursor = _make_cursor()
    cursor.close()
    with pytest.raises(Error, match="Cursor is closed"):
        cursor.fetchmany()


# ---------------------------------------------------------------------------
# Cursor – fetchone / fetchmany
# ---------------------------------------------------------------------------

def test_cursor_fetchone():
    cursor = _make_cursor()
    cursor._query_rows = iter([(1, "Alice"), (2, "Bob")])
    assert cursor.fetchone() == (1, "Alice")
    assert cursor.fetchone() == (2, "Bob")
    assert cursor.fetchone() is None


def test_cursor_fetchmany_default_size():
    cursor = _make_cursor()
    cursor.arraysize = 2
    cursor._query_rows = iter([(1,), (2,), (3,)])
    result = cursor.fetchmany()
    assert result == [(1,), (2,)]


def test_cursor_fetchmany_explicit_size():
    cursor = _make_cursor()
    cursor._query_rows = iter([(1,), (2,), (3,)])
    result = cursor.fetchmany(size=2)
    assert result == [(1,), (2,)]


def test_cursor_fetchmany_size_exceeds_rows():
    cursor = _make_cursor()
    cursor._query_rows = iter([(1,)])
    result = cursor.fetchmany(size=5)
    assert result == [(1,)]


def test_cursor_fetchmany_no_arraysize():
    cursor = _make_cursor()
    cursor.arraysize = None
    cursor._query_rows = iter([(1,), (2,), (3,)])
    result = cursor.fetchmany()
    assert result == [(1,)]


# ---------------------------------------------------------------------------
# Cursor._is_derived_query
# ---------------------------------------------------------------------------

def test_is_derived_query_simple():
    from sqlglot import tokenize
    cursor = _make_cursor()
    tokens = tokenize("SELECT * FROM users")
    assert cursor._is_derived_query(tokens) is False


def test_is_derived_query_subquery():
    from sqlglot import tokenize
    cursor = _make_cursor()
    tokens = tokenize("SELECT * FROM (SELECT * FROM users) AS vt")
    assert cursor._is_derived_query(tokens) is True


# ---------------------------------------------------------------------------
# Cursor._is_aggregation_query
# ---------------------------------------------------------------------------

def test_is_aggregation_query_count():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("SELECT COUNT(*) FROM users") is True


def test_is_aggregation_query_count_up_to():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("SELECT COUNT_UP_TO(5) FROM users") is True


def test_is_aggregation_query_sum():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("SELECT SUM(age) FROM users") is True


def test_is_aggregation_query_avg():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("SELECT AVG(age) FROM users") is True


def test_is_aggregation_query_aggregate_over():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("AGGREGATE COUNT(*) OVER (SELECT * FROM users)") is True


def test_is_aggregation_query_plain_select():
    cursor = _make_cursor()
    assert cursor._is_aggregation_query("SELECT * FROM users") is False


# ---------------------------------------------------------------------------
# Cursor._parse_aggregation_query
# ---------------------------------------------------------------------------

def test_parse_aggregation_query_aggregate_over():
    cursor = _make_cursor()
    result = cursor._parse_aggregation_query(
        "AGGREGATE COUNT(*) OVER (SELECT * FROM users WHERE age > 20)"
    )
    assert result["is_aggregate_over"] is True
    assert result["base_query"] == "SELECT * FROM users WHERE age > 20"
    assert len(result["agg_functions"]) == 1
    assert result["agg_functions"][0][0] == "COUNT"


def test_parse_aggregation_query_select_count():
    cursor = _make_cursor()
    result = cursor._parse_aggregation_query("SELECT COUNT(*) FROM users")
    assert result["is_aggregate_over"] is False
    assert "FROM users" in result["base_query"]
    assert result["agg_functions"][0][0] == "COUNT"


def test_parse_aggregation_query_multiple_agg():
    cursor = _make_cursor()
    result = cursor._parse_aggregation_query(
        "AGGREGATE COUNT(*), SUM(age) AS total_age OVER (SELECT * FROM users)"
    )
    assert result["is_aggregate_over"] is True
    assert len(result["agg_functions"]) == 2


def test_parse_aggregation_query_count_up_to():
    cursor = _make_cursor()
    result = cursor._parse_aggregation_query(
        "AGGREGATE COUNT_UP_TO(10) AS cnt OVER (SELECT * FROM tasks)"
    )
    assert result["is_aggregate_over"] is True
    assert result["agg_functions"][0] == ("COUNT_UP_TO", "10", "cnt")


def test_parse_aggregation_query_avg():
    cursor = _make_cursor()
    result = cursor._parse_aggregation_query(
        "AGGREGATE AVG(reward) AS avg_reward OVER (SELECT * FROM tasks)"
    )
    assert result["agg_functions"][0] == ("AVG", "reward", "avg_reward")


# ---------------------------------------------------------------------------
# Cursor._extract_agg_functions
# ---------------------------------------------------------------------------

def test_extract_agg_functions_count():
    cursor = _make_cursor()
    funcs = cursor._extract_agg_functions("COUNT(*)")
    assert funcs == [("COUNT", "*", "COUNT")]


def test_extract_agg_functions_count_with_alias():
    cursor = _make_cursor()
    funcs = cursor._extract_agg_functions("COUNT(*) AS total")
    assert funcs == [("COUNT", "*", "total")]


def test_extract_agg_functions_sum_avg():
    cursor = _make_cursor()
    funcs = cursor._extract_agg_functions("SUM(age) AS total, AVG(age) AS average")
    assert len(funcs) == 2
    assert funcs[0] == ("SUM", "age", "total")
    assert funcs[1] == ("AVG", "age", "average")


# ---------------------------------------------------------------------------
# Cursor._compute_aggregations
# ---------------------------------------------------------------------------

def test_compute_aggregations_count():
    cursor = _make_cursor()
    rows = [(1, "a"), (2, "b"), (3, "c")]
    fields = {"id": ("id",), "name": ("name",)}
    result_rows, result_fields = cursor._compute_aggregations(
        rows, fields, [("COUNT", "*", "cnt")]
    )
    assert result_rows == [(3,)]
    assert "cnt" in result_fields


def test_compute_aggregations_count_up_to():
    cursor = _make_cursor()
    rows = [(1,), (2,), (3,), (4,), (5,)]
    fields = {"id": ("id",)}
    result_rows, _ = cursor._compute_aggregations(
        rows, fields, [("COUNT_UP_TO", "3", "cnt")]
    )
    assert result_rows == [(3,)]


def test_compute_aggregations_sum():
    cursor = _make_cursor()
    rows = [(10,), (20,), (30,)]
    fields = {"age": ("age",)}
    result_rows, _ = cursor._compute_aggregations(
        rows, fields, [("SUM", "age", "total")]
    )
    assert result_rows == [(60,)]


def test_compute_aggregations_avg():
    cursor = _make_cursor()
    rows = [(10,), (20,), (30,)]
    fields = {"age": ("age",)}
    result_rows, _ = cursor._compute_aggregations(
        rows, fields, [("AVG", "age", "average")]
    )
    assert result_rows == [(20.0,)]


def test_compute_aggregations_sum_missing_column():
    cursor = _make_cursor()
    rows = [(10,)]
    fields = {"age": ("age",)}
    result_rows, _ = cursor._compute_aggregations(
        rows, fields, [("SUM", "nonexistent", "total")]
    )
    assert result_rows == [(0,)]


def test_compute_aggregations_avg_with_none():
    cursor = _make_cursor()
    rows = [(10,), (None,), (30,)]
    fields = {"age": ("age",)}
    result_rows, _ = cursor._compute_aggregations(
        rows, fields, [("AVG", "age", "average")]
    )
    assert result_rows == [(20.0,)]


# ---------------------------------------------------------------------------
# Cursor._needs_client_side_filter
# ---------------------------------------------------------------------------

def test_needs_client_side_filter_or():
    cursor = _make_cursor()
    assert cursor._needs_client_side_filter("SELECT * FROM users WHERE age > 10 OR name = 'x'") is True


def test_needs_client_side_filter_blob():
    cursor = _make_cursor()
    assert cursor._needs_client_side_filter("SELECT * FROM users WHERE data = BLOB('abc')") is True


def test_needs_client_side_filter_simple():
    cursor = _make_cursor()
    assert cursor._needs_client_side_filter("SELECT * FROM users WHERE age > 10") is False


# ---------------------------------------------------------------------------
# Cursor._extract_base_query_for_filter
# ---------------------------------------------------------------------------

def test_extract_base_query_for_filter():
    cursor = _make_cursor()
    result = cursor._extract_base_query_for_filter(
        "SELECT * FROM users WHERE age > 10 ORDER BY name"
    )
    assert result == "SELECT * FROM users ORDER BY name"


def test_extract_base_query_for_filter_no_where():
    cursor = _make_cursor()
    result = cursor._extract_base_query_for_filter("SELECT * FROM users")
    assert result == "SELECT * FROM users"


def test_extract_base_query_for_filter_with_limit():
    cursor = _make_cursor()
    result = cursor._extract_base_query_for_filter(
        "SELECT * FROM users WHERE age > 10 LIMIT 5"
    )
    assert result == "SELECT * FROM users LIMIT 5"


# ---------------------------------------------------------------------------
# Cursor._extract_table_only_query
# ---------------------------------------------------------------------------

def test_extract_table_only_query():
    cursor = _make_cursor()
    result = cursor._extract_table_only_query("SELECT name, age FROM users WHERE age > 10")
    assert result == "SELECT * FROM users"


def test_extract_table_only_query_no_table():
    cursor = _make_cursor()
    with pytest.raises(ProgrammingError, match="Could not extract table name"):
        cursor._extract_table_only_query("SELECT COUNT(*)")


# ---------------------------------------------------------------------------
# Cursor._parse_order_by_clause
# ---------------------------------------------------------------------------

def test_parse_order_by_clause():
    cursor = _make_cursor()
    result = cursor._parse_order_by_clause(
        "SELECT * FROM users ORDER BY name ASC, age DESC"
    )
    assert result == [("name", True), ("age", False)]


def test_parse_order_by_clause_no_direction():
    cursor = _make_cursor()
    result = cursor._parse_order_by_clause(
        "SELECT * FROM users ORDER BY name"
    )
    assert result == [("name", True)]


def test_parse_order_by_clause_none():
    cursor = _make_cursor()
    result = cursor._parse_order_by_clause("SELECT * FROM users")
    assert result == []


def test_parse_order_by_clause_with_limit():
    cursor = _make_cursor()
    result = cursor._parse_order_by_clause(
        "SELECT * FROM users ORDER BY name DESC LIMIT 10"
    )
    assert result == [("name", False)]


# ---------------------------------------------------------------------------
# Cursor._parse_limit_offset_clause
# ---------------------------------------------------------------------------

def test_parse_limit_offset_clause():
    cursor = _make_cursor()
    limit, offset = cursor._parse_limit_offset_clause(
        "SELECT * FROM users LIMIT 10 OFFSET 5"
    )
    assert limit == 10
    assert offset == 5


def test_parse_limit_offset_clause_limit_only():
    cursor = _make_cursor()
    limit, offset = cursor._parse_limit_offset_clause("SELECT * FROM users LIMIT 10")
    assert limit == 10
    assert offset == 0


def test_parse_limit_offset_clause_neither():
    cursor = _make_cursor()
    limit, offset = cursor._parse_limit_offset_clause("SELECT * FROM users")
    assert limit is None
    assert offset == 0


# ---------------------------------------------------------------------------
# Cursor._apply_client_side_order_by
# ---------------------------------------------------------------------------

def test_apply_client_side_order_by_asc():
    cursor = _make_cursor()
    rows = [(3, "c"), (1, "a"), (2, "b")]
    fields = {"id": ("id",), "name": ("name",)}
    result = cursor._apply_client_side_order_by(rows, fields, [("id", True)])
    assert result == [(1, "a"), (2, "b"), (3, "c")]


def test_apply_client_side_order_by_desc():
    cursor = _make_cursor()
    rows = [(1, "a"), (3, "c"), (2, "b")]
    fields = {"id": ("id",), "name": ("name",)}
    result = cursor._apply_client_side_order_by(rows, fields, [("id", False)])
    assert result == [(3, "c"), (2, "b"), (1, "a")]


def test_apply_client_side_order_by_with_none():
    cursor = _make_cursor()
    rows = [(None, "x"), (1, "a"), (None, "y")]
    fields = {"id": ("id",), "name": ("name",)}
    result = cursor._apply_client_side_order_by(rows, fields, [("id", True)])
    assert result[0] == (1, "a")
    # None values sort to end
    assert result[1][0] is None
    assert result[2][0] is None


def test_apply_client_side_order_by_empty():
    cursor = _make_cursor()
    result = cursor._apply_client_side_order_by([], {}, [("id", True)])
    assert result == []


def test_apply_client_side_order_by_no_keys():
    cursor = _make_cursor()
    rows = [(3,), (1,), (2,)]
    result = cursor._apply_client_side_order_by(rows, {"id": ("id",)}, [])
    assert result == [(3,), (1,), (2,)]


# ---------------------------------------------------------------------------
# Cursor._parse_literal
# ---------------------------------------------------------------------------

def test_parse_literal_string_single_quotes():
    cursor = _make_cursor()
    assert cursor._parse_literal("'hello'") == "hello"


def test_parse_literal_string_double_quotes():
    cursor = _make_cursor()
    assert cursor._parse_literal('"world"') == "world"


def test_parse_literal_true():
    cursor = _make_cursor()
    assert cursor._parse_literal("TRUE") is True
    assert cursor._parse_literal("true") is True


def test_parse_literal_false():
    cursor = _make_cursor()
    assert cursor._parse_literal("FALSE") is False
    assert cursor._parse_literal("false") is False


def test_parse_literal_null():
    cursor = _make_cursor()
    assert cursor._parse_literal("NULL") is None
    assert cursor._parse_literal("null") is None


def test_parse_literal_integer():
    cursor = _make_cursor()
    assert cursor._parse_literal("42") == 42


def test_parse_literal_float():
    cursor = _make_cursor()
    assert cursor._parse_literal("3.14") == 3.14


def test_parse_literal_datetime():
    cursor = _make_cursor()
    result = cursor._parse_literal("DATETIME('2023-01-01T00:00:00Z')")
    assert isinstance(result, datetime)
    assert result.year == 2023


def test_parse_literal_datetime_with_microseconds():
    cursor = _make_cursor()
    result = cursor._parse_literal("DATETIME('2023-01-01T12:30:45.123456Z')")
    assert isinstance(result, datetime)
    assert result.microsecond == 123456


def test_parse_literal_unrecognized():
    cursor = _make_cursor()
    assert cursor._parse_literal("some_identifier") == "some_identifier"


# ---------------------------------------------------------------------------
# Cursor._parse_value_list
# ---------------------------------------------------------------------------

def test_parse_value_list():
    cursor = _make_cursor()
    result = cursor._parse_value_list("'a', 'b', 'c'")
    assert result == ["a", "b", "c"]


def test_parse_value_list_numbers():
    cursor = _make_cursor()
    result = cursor._parse_value_list("1, 2, 3")
    assert result == [1, 2, 3]


# ---------------------------------------------------------------------------
# Cursor._eval_simple_condition
# ---------------------------------------------------------------------------

def test_eval_simple_condition_eq():
    cursor = _make_cursor()
    context = {"name": "Alice", "age": 25}
    assert cursor._eval_simple_condition(context, "name = 'Alice'") is True
    assert cursor._eval_simple_condition(context, "name = 'Bob'") is False


def test_eval_simple_condition_neq():
    cursor = _make_cursor()
    context = {"name": "Alice"}
    assert cursor._eval_simple_condition(context, "name != 'Bob'") is True
    assert cursor._eval_simple_condition(context, "name != 'Alice'") is False


def test_eval_simple_condition_gt():
    cursor = _make_cursor()
    context = {"age": 25}
    assert cursor._eval_simple_condition(context, "age > 20") is True
    assert cursor._eval_simple_condition(context, "age > 30") is False


def test_eval_simple_condition_gte():
    cursor = _make_cursor()
    context = {"age": 25}
    assert cursor._eval_simple_condition(context, "age >= 25") is True
    assert cursor._eval_simple_condition(context, "age >= 26") is False


def test_eval_simple_condition_lt():
    cursor = _make_cursor()
    context = {"age": 25}
    assert cursor._eval_simple_condition(context, "age < 30") is True
    assert cursor._eval_simple_condition(context, "age < 20") is False


def test_eval_simple_condition_lte():
    cursor = _make_cursor()
    context = {"age": 25}
    assert cursor._eval_simple_condition(context, "age <= 25") is True
    assert cursor._eval_simple_condition(context, "age <= 24") is False


def test_eval_simple_condition_in():
    cursor = _make_cursor()
    context = {"country": "Arland"}
    assert cursor._eval_simple_condition(context, "country IN ('Arland', 'Britannia')") is True
    assert cursor._eval_simple_condition(context, "country IN ('Japan', 'US')") is False


def test_eval_simple_condition_not_in():
    cursor = _make_cursor()
    context = {"country": "Arland"}
    assert cursor._eval_simple_condition(context, "country NOT IN ('Japan', 'US')") is True
    assert cursor._eval_simple_condition(context, "country NOT IN ('Arland', 'Japan')") is False


def test_eval_simple_condition_null_field():
    cursor = _make_cursor()
    context = {"age": None}
    assert cursor._eval_simple_condition(context, "age > 10") is False
    assert cursor._eval_simple_condition(context, "age < 10") is False
    assert cursor._eval_simple_condition(context, "age >= 10") is False
    assert cursor._eval_simple_condition(context, "age <= 10") is False


def test_eval_simple_condition_key_eq():
    cursor = _make_cursor()
    context = {"key": [{"kind": "users", "name": "alice_id"}]}
    assert cursor._eval_simple_condition(
        context, "__key__ = KEY(users, 'alice_id')"
    ) is True
    assert cursor._eval_simple_condition(
        context, "__key__ = KEY(users, 'bob_id')"
    ) is False


def test_eval_simple_condition_key_eq_numeric():
    cursor = _make_cursor()
    context = {"key": [{"kind": "users", "id": "123"}]}
    assert cursor._eval_simple_condition(
        context, "__key__ = KEY(users, 123)"
    ) is True


def test_eval_simple_condition_blob_eq():
    cursor = _make_cursor()
    context = {"data": b"hello"}
    assert cursor._eval_simple_condition(context, "data = BLOB('hello')") is True
    assert cursor._eval_simple_condition(context, "data = BLOB('world')") is False


def test_eval_simple_condition_blob_neq():
    cursor = _make_cursor()
    context = {"data": b"hello"}
    assert cursor._eval_simple_condition(context, "data != BLOB('world')") is True
    assert cursor._eval_simple_condition(context, "data != BLOB('hello')") is False


def test_eval_simple_condition_default():
    cursor = _make_cursor()
    context = {"x": 1}
    # Unmatched condition defaults to True
    assert cursor._eval_simple_condition(context, "some_unrecognizable_thing") is True


# ---------------------------------------------------------------------------
# Cursor._eval_condition (compound)
# ---------------------------------------------------------------------------

def test_eval_condition_and():
    cursor = _make_cursor()
    context = {"age": 25, "name": "Alice"}
    assert cursor._eval_condition(context, "age > 20 AND name = 'Alice'") is True
    assert cursor._eval_condition(context, "age > 30 AND name = 'Alice'") is False


def test_eval_condition_or():
    cursor = _make_cursor()
    context = {"age": 25, "name": "Alice"}
    assert cursor._eval_condition(context, "age > 30 OR name = 'Alice'") is True
    assert cursor._eval_condition(context, "age > 30 OR name = 'Bob'") is False


def test_eval_condition_parentheses():
    cursor = _make_cursor()
    context = {"age": 25, "name": "Alice"}
    assert cursor._eval_condition(context, "(age > 20)") is True
    assert cursor._eval_condition(context, "(age > 20 AND name = 'Alice')") is True


# ---------------------------------------------------------------------------
# Cursor._split_on_operator
# ---------------------------------------------------------------------------

def test_split_on_operator_and():
    cursor = _make_cursor()
    parts = cursor._split_on_operator("a = 1 AND b = 2", "AND")
    assert len(parts) == 2
    assert parts[0].strip() == "a = 1"
    assert parts[1].strip() == "b = 2"


def test_split_on_operator_with_parens():
    cursor = _make_cursor()
    parts = cursor._split_on_operator("(a = 1 AND b = 2) OR c = 3", "OR")
    assert len(parts) == 2
    assert parts[0].strip() == "(a = 1 AND b = 2)"
    assert parts[1].strip() == "c = 3"


# ---------------------------------------------------------------------------
# Cursor._apply_client_side_filter
# ---------------------------------------------------------------------------

def test_apply_client_side_filter():
    cursor = _make_cursor()
    rows = [(25, "Alice"), (15, "Bob"), (30, "Charlie")]
    fields = {"age": ("age",), "name": ("name",)}
    result = cursor._apply_client_side_filter(
        rows, fields, "SELECT * FROM users WHERE age > 20"
    )
    assert len(result) == 2
    assert (25, "Alice") in result
    assert (30, "Charlie") in result


def test_apply_client_side_filter_no_where():
    cursor = _make_cursor()
    rows = [(1,), (2,)]
    fields = {"id": ("id",)}
    result = cursor._apply_client_side_filter(rows, fields, "SELECT * FROM users")
    assert result == [(1,), (2,)]


# ---------------------------------------------------------------------------
# Cursor._parse_select_columns
# ---------------------------------------------------------------------------

def test_parse_select_columns_star():
    cursor = _make_cursor()
    assert cursor._parse_select_columns("SELECT * FROM users") is None


def test_parse_select_columns_specific():
    cursor = _make_cursor()
    result = cursor._parse_select_columns("SELECT name, age FROM users")
    assert result == ["name", "age"]


def test_parse_select_columns_with_alias():
    cursor = _make_cursor()
    result = cursor._parse_select_columns("SELECT name AS n FROM users")
    assert result == ["name"]


def test_parse_select_columns_id_maps_to_key():
    cursor = _make_cursor()
    result = cursor._parse_select_columns("SELECT id, name FROM users")
    assert "__key__" in result


# ---------------------------------------------------------------------------
# Cursor._convert_sql_to_gql
# ---------------------------------------------------------------------------

def test_convert_sql_to_gql_simple():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql("SELECT * FROM users")
    assert "SELECT" in result
    assert "FROM" in result
    assert "users" in result


def test_convert_sql_to_gql_neq_operator():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql("SELECT * FROM users WHERE age <> 10")
    assert "!=" in result
    assert "<>" not in result


def test_convert_sql_to_gql_not_in():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql(
        "SELECT * FROM users WHERE NOT name IN ('Alice', 'Bob')"
    )
    assert "name NOT IN" in result


def test_convert_sql_to_gql_in_array():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql(
        "SELECT * FROM users WHERE name IN ('Alice', 'Bob')"
    )
    assert "ARRAY" in result


def test_convert_sql_to_gql_aggregate_passthrough():
    cursor = _make_cursor()
    stmt = "AGGREGATE COUNT(*) OVER (SELECT * FROM users)"
    assert cursor._convert_sql_to_gql(stmt) == stmt


def test_convert_sql_to_gql_removes_aliases():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql("SELECT name AS n FROM users")
    assert "AS" not in result.upper().split("FROM")[0]


def test_convert_sql_to_gql_table_prefix_removal():
    cursor = _make_cursor()
    result = cursor._convert_sql_to_gql("SELECT users.name FROM users")
    # Table prefix should be removed
    assert "users.name" not in result


# ---------------------------------------------------------------------------
# Cursor._substitute_parameters
# ---------------------------------------------------------------------------

def test_substitute_parameters_string():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE name = :name",
        {"name": "Alice"},
    )
    assert "'Alice'" in result
    assert ":name" not in result


def test_substitute_parameters_integer():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE age = :age",
        {"age": 25},
    )
    assert "25" in result


def test_substitute_parameters_float():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM tasks WHERE reward = :reward",
        {"reward": 100.5},
    )
    assert "100.5" in result


def test_substitute_parameters_none():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE settings = :settings",
        {"settings": None},
    )
    assert "NULL" in result


def test_substitute_parameters_bool():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM tasks WHERE is_done = :done",
        {"done": True},
    )
    assert "true" in result


def test_substitute_parameters_datetime():
    cursor = _make_cursor()
    dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE create_time = :ct",
        {"ct": dt},
    )
    assert "DATETIME(" in result


def test_substitute_parameters_other_type():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE data = :data",
        {"data": [1, 2, 3]},
    )
    assert "'[1, 2, 3]'" in result


def test_substitute_parameters_escapes_quotes():
    cursor = _make_cursor()
    result = cursor._substitute_parameters(
        "SELECT * FROM users WHERE name = :name",
        {"name": "O'Brien"},
    )
    assert "O''Brien" in result


# ---------------------------------------------------------------------------
# Cursor._is_orm_id_query
# ---------------------------------------------------------------------------

def test_is_orm_id_query_true():
    cursor = _make_cursor()
    assert cursor._is_orm_id_query(
        "SELECT users.id AS users_id, users.name FROM users WHERE users.id = :pk_1"
    ) is True


def test_is_orm_id_query_false():
    cursor = _make_cursor()
    assert cursor._is_orm_id_query("SELECT * FROM users") is False


# ---------------------------------------------------------------------------
# Cursor._is_missing_index_error
# ---------------------------------------------------------------------------

def test_is_missing_index_error_true():
    cursor = _make_cursor()
    response = MagicMock()
    response.status_code = 409
    response.json.return_value = {
        "error": {"message": "no matching index found", "status": "FAILED_PRECONDITION"}
    }
    assert cursor._is_missing_index_error(response) is True


def test_is_missing_index_error_false():
    cursor = _make_cursor()
    response = MagicMock()
    response.status_code = 200
    assert cursor._is_missing_index_error(response) is False


def test_is_missing_index_error_bad_json():
    cursor = _make_cursor()
    response = MagicMock()
    response.status_code = 400
    response.json.side_effect = ValueError
    response.text = "no matching index error"
    assert cursor._is_missing_index_error(response) is True


# ---------------------------------------------------------------------------
# Cursor._parse_insert_value
# ---------------------------------------------------------------------------

def test_parse_insert_value_string():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.string("hello")
    assert cursor._parse_insert_value(val, {}) == "hello"


def test_parse_insert_value_number():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.number(42)
    assert cursor._parse_insert_value(val, {}) == 42


def test_parse_insert_value_float():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.number("3.14")
    assert cursor._parse_insert_value(val, {}) == 3.14


def test_parse_insert_value_null():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Null()
    assert cursor._parse_insert_value(val, {}) is None


def test_parse_insert_value_boolean():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Boolean(this=True)
    assert cursor._parse_insert_value(val, {}) is True


def test_parse_insert_value_placeholder():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Placeholder(this="name")
    assert cursor._parse_insert_value(val, {"name": "Alice"}) == "Alice"


# ---------------------------------------------------------------------------
# Cursor._parse_update_value
# ---------------------------------------------------------------------------

def test_parse_update_value_string():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.string("updated")
    assert cursor._parse_update_value(val, {}) == "updated"


def test_parse_update_value_number():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.number(99)
    assert cursor._parse_update_value(val, {}) == 99


def test_parse_update_value_null():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Null()
    assert cursor._parse_update_value(val, {}) is None


def test_parse_update_value_boolean():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Boolean(this=False)
    assert cursor._parse_update_value(val, {}) is False


def test_parse_update_value_placeholder():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Placeholder(this="age")
    assert cursor._parse_update_value(val, {"age": 30}) == 30


def test_parse_update_value_placeholder_missing():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Placeholder(this="missing")
    assert cursor._parse_update_value(val, {}) is None


# ---------------------------------------------------------------------------
# Cursor._parse_key_value
# ---------------------------------------------------------------------------

def test_parse_key_value_literal_number():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Literal.number(42)
    assert cursor._parse_key_value(val, {}) == 42


def test_parse_key_value_placeholder():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Placeholder(this="pk_1")
    assert cursor._parse_key_value(val, {"pk_1": 99}) == 99


def test_parse_key_value_placeholder_colon():
    from sqlglot import exp
    cursor = _make_cursor()
    val = exp.Placeholder(this=":pk_1")
    assert cursor._parse_key_value(val, {"pk_1": 77}) == 77


# ---------------------------------------------------------------------------
# Cursor._extract_key_id_from_where
# ---------------------------------------------------------------------------

def test_extract_key_id_from_where():
    from sqlglot import parse_one
    cursor = _make_cursor()
    parsed = parse_one("SELECT * FROM users WHERE id = 42")
    where = parsed.args.get("where")
    result = cursor._extract_key_id_from_where(where, {})
    assert result == 42


def test_extract_key_id_from_where_param():
    from sqlglot import parse_one
    cursor = _make_cursor()
    parsed = parse_one("SELECT * FROM users WHERE id = :pk_1")
    where = parsed.args.get("where")
    result = cursor._extract_key_id_from_where(where, {"pk_1": 99})
    assert result == 99


# ---------------------------------------------------------------------------
# ParseEntity.parse_properties
# ---------------------------------------------------------------------------

def test_parse_properties_null():
    _, prop_type = ParseEntity.parse_properties("x", {"nullValue": None})
    assert prop_type is not None


def test_parse_properties_boolean():
    val, _ = ParseEntity.parse_properties("x", {"booleanValue": True})
    assert val is True


def test_parse_properties_integer():
    val, _ = ParseEntity.parse_properties("x", {"integerValue": "42"})
    assert val == 42


def test_parse_properties_double():
    val, _ = ParseEntity.parse_properties("x", {"doubleValue": 3.14})
    assert val == 3.14


def test_parse_properties_string():
    val, _ = ParseEntity.parse_properties("x", {"stringValue": "hello"})
    assert val == "hello"


def test_parse_properties_timestamp_utc():
    val, _ = ParseEntity.parse_properties(
        "x", {"timestampValue": "2025-01-01T00:00:00Z"}
    )
    assert isinstance(val, datetime)
    assert val.year == 2025


def test_parse_properties_timestamp_no_z():
    val, _ = ParseEntity.parse_properties(
        "x", {"timestampValue": "2025-06-15T12:30:00+00:00"}
    )
    assert isinstance(val, datetime)


def test_parse_properties_blob():
    import base64
    encoded = base64.b64encode(b"binary data").decode()
    val, _ = ParseEntity.parse_properties("x", {"blobValue": encoded})
    assert val == b"binary data"


def test_parse_properties_geopoint():
    val, _ = ParseEntity.parse_properties(
        "x", {"geoPointValue": {"latitude": 25.0, "longitude": 121.5}}
    )
    assert val == {"latitude": 25.0, "longitude": 121.5}


def test_parse_properties_key():
    val, _ = ParseEntity.parse_properties(
        "x",
        {"keyValue": {"path": [{"kind": "users", "name": "alice"}]}},
    )
    assert val == [{"kind": "users", "name": "alice"}]


def test_parse_properties_array():
    val, _ = ParseEntity.parse_properties(
        "x",
        {"arrayValue": {"values": [
            {"stringValue": "a"},
            {"stringValue": "b"},
        ]}},
    )
    assert val == ["a", "b"]


def test_parse_properties_array_empty():
    val, _ = ParseEntity.parse_properties(
        "x", {"arrayValue": {}}
    )
    assert val == []


def test_parse_properties_entity():
    val, _ = ParseEntity.parse_properties(
        "x",
        {"entityValue": {"properties": {"nested": {"stringValue": "value"}}}},
    )
    assert "nested" in val


def test_parse_properties_entity_empty():
    val, _ = ParseEntity.parse_properties(
        "x", {"entityValue": {}}
    )
    assert val == {}


def test_parse_properties_dict():
    val, _ = ParseEntity.parse_properties(
        "x", {"dictValue": {"key": "value"}}
    )
    assert val == {"key": "value"}


# ---------------------------------------------------------------------------
# ParseEntity.parse
# ---------------------------------------------------------------------------

def test_parse_entity_all_columns():
    data = [
        {
            "entity": {
                "key": {"path": [{"kind": "users", "name": "alice"}]},
                "properties": {
                    "name": {"stringValue": "Alice"},
                    "age": {"integerValue": "25"},
                },
            }
        }
    ]
    rows, fields = ParseEntity.parse(data, None)
    assert len(rows) == 1
    assert "key" in fields
    assert "name" in fields
    assert "age" in fields


def test_parse_entity_selected_columns():
    data = [
        {
            "entity": {
                "key": {"path": [{"kind": "users", "name": "alice"}]},
                "properties": {
                    "name": {"stringValue": "Alice"},
                    "age": {"integerValue": "25"},
                },
            }
        }
    ]
    rows, fields = ParseEntity.parse(data, ["name"])
    assert len(rows) == 1
    assert "name" in fields
    assert "age" not in fields
    assert "key" not in fields


def test_parse_entity_key_column():
    data = [
        {
            "entity": {
                "key": {"path": [{"kind": "users", "name": "alice"}]},
                "properties": {
                    "name": {"stringValue": "Alice"},
                },
            }
        }
    ]
    rows, fields = ParseEntity.parse(data, ["__key__", "name"])
    assert "key" in fields
    assert "name" in fields


def test_parse_entity_missing_property():
    data = [
        {
            "entity": {
                "key": {"path": []},
                "properties": {
                    "name": {"stringValue": "Alice"},
                },
            }
        },
        {
            "entity": {
                "key": {"path": []},
                "properties": {},
            }
        },
    ]
    rows, fields = ParseEntity.parse(data, None)
    # Second row should have None for missing "name"
    assert rows[1][1] is None


# ---------------------------------------------------------------------------
# Cursor._create_schema_from_df
# ---------------------------------------------------------------------------

def test_create_schema_from_df():
    import pandas as pd
    cursor = _make_cursor()
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, 30],
        "score": [95.5, 88.0],
        "active": [True, False],
    })
    schema = cursor._create_schema_from_df(df)
    assert len(schema) == 4
    assert schema[0].name == "name"
    assert schema[1].name == "age"


def test_create_schema_from_df_datetime():
    import pandas as pd
    cursor = _make_cursor()
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2025-01-01", "2025-06-01"]),
    })
    schema = cursor._create_schema_from_df(df)
    assert len(schema) == 1


def test_create_schema_from_df_object_type():
    import pandas as pd
    cursor = _make_cursor()
    df = pd.DataFrame({
        "data": [{"a": 1}, {"b": 2}],
    })
    schema = cursor._create_schema_from_df(df)
    assert len(schema) == 1


# ---------------------------------------------------------------------------
# Cursor._set_description
# ---------------------------------------------------------------------------

def test_set_description():
    cursor = _make_cursor()
    schema = (Column("name", "STRING", None, None, None, None, True),)
    cursor._set_description(schema)
    assert cursor.description == schema


def test_set_description_empty():
    cursor = _make_cursor()
    cursor._set_description()
    assert cursor.description == ()


# ---------------------------------------------------------------------------
# DBAPI exception hierarchy
# ---------------------------------------------------------------------------

def test_exception_hierarchy():
    from sqlalchemy_datastore.datastore_dbapi import (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        OperationalError,
        ProgrammingError,
    )
    from sqlalchemy_datastore.datastore_dbapi import (
        Warning as DBWarning,
    )
    assert issubclass(InterfaceError, Error)
    assert issubclass(DatabaseError, Error)
    assert issubclass(DataError, DatabaseError)
    assert issubclass(OperationalError, DatabaseError)
    assert issubclass(IntegrityError, DatabaseError)
    assert issubclass(InternalError, DatabaseError)
    assert issubclass(ProgrammingError, DatabaseError)
    assert issubclass(DBWarning, Exception)
