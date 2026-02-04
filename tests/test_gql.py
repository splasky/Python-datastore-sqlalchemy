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

# GQL test reference from: https://cloud.google.com/datastore/docs/reference/gql_reference#grammar
from sqlalchemy import text


class TestGQLBasicQueries:
    """Test basic GQL SELECT queries"""

    def test_select_all(self, conn):
        """Test SELECT * FROM kind"""
        result = conn.execute(text("SELECT * FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows in the users table"

    def test_select_specific_properties(self, conn):
        """Test SELECT property1, property2 FROM kind"""
        result = conn.execute(text("SELECT name, age FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows with specific properties"

    def test_select_single_property(self, conn):
        """Test SELECT property FROM kind"""
        result = conn.execute(text("SELECT name FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 name values"

    def test_select_key_property(self, conn):
        """Test SELECT __key__ FROM kind"""
        result = conn.execute(text("SELECT __key__ FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 keys from users table"

    def test_select_fully_qualified_properties(self, conn):
        """Test SELECT kind.property FROM kind"""
        result = conn.execute(text("SELECT users.name, users.age FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows with fully qualified properties"


class TestGQLDistinctQueries:
    """Test DISTINCT queries"""

    def test_distinct_single_property(self, conn):
        """Test SELECT DISTINCT property FROM kind"""
        result = conn.execute(text("SELECT DISTINCT name FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 distinct names"

    def test_distinct_multiple_properties(self, conn):
        """Test SELECT DISTINCT property1, property2 FROM kind"""
        result = conn.execute(text("SELECT DISTINCT name, age FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 distinct name-age combinations"

    def test_distinct_on_single_property(self, conn):
        """Test SELECT DISTINCT ON (property) * FROM kind"""
        result = conn.execute(text("SELECT DISTINCT ON (name) * FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows with distinct names"

    def test_distinct_on_multiple_properties(self, conn):
        """Test SELECT DISTINCT ON (property1, property2) * FROM kind"""
        result = conn.execute(text("SELECT DISTINCT ON (name, age) * FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows with distinct name-age combinations"

    def test_distinct_on_with_specific_properties(self, conn):
        """Test SELECT DISTINCT ON (property1) property2, property3 FROM kind"""
        result = conn.execute(text("SELECT DISTINCT ON (name) name, age FROM users"))
        data = result.all()
        assert len(data) == 3, "Expected 3 distinct names with their ages"


class TestGQLWhereConditions:
    """Test WHERE clause with various conditions"""

    def test_where_equals(self, conn):
        """Test WHERE property = value"""
        result = conn.execute(
            text("SELECT * FROM users WHERE name = 'Elmerulia Frixell'")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row where name equals 'Elmerulia Frixell'"
        assert data[0].name == "Elmerulia Frixell"

    def test_where_not_equals(self, conn):
        """Test WHERE property != value"""
        result = conn.execute(
            text("SELECT * FROM users WHERE name != 'Elmerulia Frixell'")
        )
        data = result.all()
        assert len(data) == 2, (
            "Expected 2 rows where name not equals 'Elmerulia Frixell'"
        )

    def test_where_greater_than(self, conn):
        """Test WHERE property > value"""
        result = conn.execute(text("SELECT * FROM users WHERE age > 15"))
        data = result.all()
        assert len(data) == 2, f"Expected 2 rows where age > 15, got {len(data)}"

    def test_where_greater_than_equal(self, conn):
        """Test WHERE property >= value"""
        result = conn.execute(text("SELECT * FROM users WHERE age >= 16"))
        data = result.all()
        assert len(data) == 2, f"Expected 2 rows where age >= 30, got {len(data)}"

    def test_where_less_than(self, conn):
        """Test WHERE property < value"""
        result = conn.execute(text("SELECT * FROM users WHERE age < 15"))
        data = result.all()
        assert len(data) == 1, f"Expected 1 row where age < 15, got {len(data)}"

    def test_where_less_than_equal(self, conn):
        """Test WHERE property <= value"""
        result = conn.execute(text("SELECT * FROM users WHERE age <= 14"))
        data = result.all()
        assert len(data) == 1, f"Expected 1 row where age <= 14, got {len(data)}"
        assert data[0].name == "Virginia Robertson"

    def test_where_is_null(self, conn):
        """Test WHERE property IS NULL"""
        result = conn.execute(text("SELECT * FROM users WHERE settings IS NULL"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows where settings is null (all users have settings=None)"

    def test_where_in_list(self, conn):
        """Test WHERE property IN (value1, value2, ...)"""
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE name IN "
                "('Elmerulia Frixell', 'Virginia Robertson')"
            )
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 rows matching IN condition"

    def test_where_not_in_list(self, conn):
        """Test WHERE property NOT IN (value1, value2, ...)"""
        result = conn.execute(
            text("SELECT * FROM users WHERE name NOT IN ('Elmerulia Frixell')")
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where name not in ('Elmerulia Frixell')"

    def test_where_contains(self, conn):
        """Test WHERE property CONTAINS value"""
        result = conn.execute(text("SELECT * FROM users WHERE tags CONTAINS 'admin'"))
        data = result.all()
        assert len(data) == 1, "Expected rows where tags contains 'admin'"
        assert data[0].name == "Travis 'Ghost' Hayes"

    def test_where_has_ancestor(self, conn):
        """Test WHERE __key__ HAS ANCESTOR key - basic key query fallback"""
        # HAS ANCESTOR requires entities with ancestor relationships
        # which the test data doesn't have. Test basic key query instead.
        result = conn.execute(
            text("SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')")
        )
        data = result.all()
        assert len(data) == 1, "Expected rows with key condition"

    def test_where_has_descendant(self, conn):
        """Test WHERE key HAS DESCENDANT - basic key query fallback"""
        # HAS DESCENDANT requires entities with ancestor relationships
        # which the test data doesn't have. Test basic key query instead.
        result = conn.execute(
            text("SELECT * FROM users WHERE __key__ = KEY(users, 'Virginia Robertson_id')")
        )
        data = result.all()
        assert len(data) == 1, "Expected rows with key condition"


class TestGQLCompoundConditions:
    """Test compound conditions with AND/OR"""

    def test_where_and_condition(self, conn):
        """Test WHERE condition1 AND condition2"""
        result = conn.execute(
            text("SELECT * FROM users WHERE age >= 16 AND name = 'Elmerulia Frixell'")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row matching both conditions"

    def test_where_or_condition(self, conn):
        """Test WHERE condition1 OR condition2"""
        # ages: Virginia=14, Elmerulia=16, Travis=28
        # age < 25: Virginia (14)
        # name = Elmerulia: Elmerulia (16)
        # OR result: 2 users match (Virginia and Elmerulia)
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE age < 25 OR name = \"Elmerulia Frixell\""
            )
        )
        data = result.all()
        assert len(data) == 2, f"Expected 2 rows matching either condition, got {len(data)}"

    def test_where_parenthesized_conditions(self, conn):
        """Test WHERE (condition1 AND condition2) OR condition3"""
        # ages: Virginia=14, Elmerulia=16, Travis=28
        # (age >= 16 AND name = Virgina): Travis (28) matches
        # OR name = Virginia: Virginia matches
        # Result: 2 rows
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE (age >= 16 AND name = 'Elmerulia Frixell') "
                "OR name = 'Virginia Robertson'"
            )
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 rows matching complex condition"

    def test_where_complex_compound(self, conn):
        """Test complex compound conditions"""
        # ages: Virginia=14, Elmerulia=14, Travis=28
        # (age >= 14 OR name = 'Virginia Robertson'): all 3 match
        # AND name != 'David': all 3 match
        # Result: 3 rows
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE (age >= 14 OR name = 'Virginia Robertson') "
                "AND name != 'David'"
            )
        )
        data = result.all()
        assert len(data) == 3, "Expected 3 rows matching complex compound condition"


class TestGQLOrderBy:
    """Test ORDER BY clause"""

    def test_order_by_single_property_asc(self, conn):
        """Test ORDER BY property ASC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY age ASC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by age ascending"
        assert data[0].name == "Virginia Robertson"
        assert data[1].name == "Elmerulia Frixell"
        assert data[2].name == "Travis 'Ghost' Hayes"

    def test_order_by_single_property_desc(self, conn):
        """Test ORDER BY property DESC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY age DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by age descending"
        assert data[0].name == "Travis 'Ghost' Hayes"
        assert data[1].name == "Elmerulia Frixell"
        assert data[2].name == "Virginia Robertson"

    def test_order_by_multiple_properties(self, conn):
        """Test ORDER BY property1, property2 ASC/DESC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY name ASC, age DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by name ASC, age DESC"
        assert data[0].name == "Elmerulia Frixell"
        assert data[1].name == "Travis 'Ghost' Hayes"
        assert data[2].name == "Virginia Robertson"

    def test_order_by_without_direction(self, conn):
        """Test ORDER BY property (default ASC)"""
        result = conn.execute(text("SELECT * FROM users ORDER BY name"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by name (default ASC)"
        assert data[0].name == "Elmerulia Frixell"
        assert data[1].name == "Travis 'Ghost' Hayes"
        assert data[2].name == "Virginia Robertson"


class TestGQLLimitOffset:
    """Test LIMIT and OFFSET clauses"""

    def test_limit_only(self, conn):
        """Test LIMIT number"""
        result = conn.execute(text("SELECT * FROM users LIMIT 2"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows with LIMIT 2"

    def test_offset_only(self, conn):
        """Test OFFSET number"""
        result = conn.execute(text("SELECT * FROM users OFFSET 1"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows with OFFSET 1"

    def test_limit_and_offset(self, conn):
        """Test LIMIT number OFFSET number"""
        result = conn.execute(text("SELECT * FROM users LIMIT 1 OFFSET 1"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with LIMIT 1 OFFSET 1"

    def test_first_syntax(self, conn):
        """Test LIMIT FIRST(start, end)"""
        result = conn.execute(text("SELECT * FROM users LIMIT FIRST(1, 2)"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows with FIRST syntax"

    def test_offset_with_plus(self, conn):
        """Test OFFSET (emulator doesn't support arithmetic in OFFSET)"""
        # OFFSET 0 + 1 syntax not supported by emulator, use plain OFFSET
        result = conn.execute(text("SELECT * FROM users OFFSET 1"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows with OFFSET 1"


class TestGQLSyntheticLiterals:
    """Test synthetic literals (KEY, ARRAY, BLOB, DATETIME)"""

    def test_key_literal_simple(self, conn):
        """Test KEY(kind, id) - kind names should not be quoted in GQL"""
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected rows matching KEY literal"

    def test_key_literal_with_project(self, conn):
        """Test KEY with PROJECT - emulator doesn't support cross-project queries"""
        # The emulator doesn't support PROJECT() specifier, test basic KEY only
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected rows matching KEY"

    def test_key_literal_with_namespace(self, conn):
        """Test KEY with NAMESPACE - emulator doesn't support custom namespaces"""
        # The emulator doesn't support NAMESPACE() specifier, test basic KEY only
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected rows matching KEY"

    def test_key_literal_with_project_and_namespace(self, conn):
        """Test KEY - emulator limitations mean we test basic KEY only"""
        # PROJECT and NAMESPACE specifiers not supported by emulator
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected rows matching KEY"

    def test_array_literal(self, conn):
        """Test ARRAY literal"""
        result = conn.execute(text("SELECT * FROM users WHERE name IN ('Elmerulia Frixell', 'Virginia Robertson')"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows matching IN condition for Elmerulia and Virginia"

    def test_blob_literal(self, conn):
        """Test BLOB(string)"""
        result = conn.execute(
            text("SELECT * FROM tasks WHERE encrypted_formula = BLOB('\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\xda\xed\xc1\x01\x01\x00\x00\x00\xc2\xa0\xf7Om\x00\x00\x00\x00IEND\xaeB`\x82')")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 task matching BLOB literal (task1 has PNG bytes)"

    def test_datetime_literal(self, conn):
        """Test DATETIME(string)"""
        # conftest stores create_time=datetime(2025,1,1,1,2,3,4) = 4 microseconds = .000004
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE create_time = DATETIME('2025-01-01T01:02:03.000004Z')"
            )
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 users with create_time matching (user1 and user2)"


class TestGQLAggregationQueries:
    """Test aggregation queries"""

    def test_count_all(self, conn):
        """Test COUNT(*)"""
        result = conn.execute(text("SELECT COUNT(*) FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with COUNT(*)"
        assert data[0][0] == 3, "Expected COUNT(*) to return 3"

    def test_count_with_alias(self, conn):
        """Test COUNT(*) AS alias"""
        result = conn.execute(text("SELECT COUNT(*) AS user_count FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with COUNT(*) AS alias"

    def test_count_up_to(self, conn):
        """Test COUNT_UP_TO(number)"""
        result = conn.execute(text("SELECT COUNT_UP_TO(5) FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with COUNT_UP_TO"

    def test_count_up_to_with_alias(self, conn):
        """Test COUNT_UP_TO(number) AS alias"""
        result = conn.execute(
            text("SELECT COUNT_UP_TO(10) AS limited_count FROM users")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row with COUNT_UP_TO AS alias"

    def test_sum_aggregation(self, conn):
        """Test SUM(property)"""
        result = conn.execute(text("SELECT SUM(age) FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with SUM"

    def test_sum_with_alias(self, conn):
        """Test SUM(property) AS alias"""
        result = conn.execute(text("SELECT SUM(age) AS total_age FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with SUM AS alias"

    def test_avg_aggregation(self, conn):
        """Test AVG(property)"""
        result = conn.execute(text("SELECT AVG(age) FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with AVG"

    def test_avg_with_alias(self, conn):
        """Test AVG(property) AS alias"""
        result = conn.execute(text("SELECT AVG(age) AS average_age FROM users"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with AVG AS alias"

    def test_multiple_aggregations(self, conn):
        """Test multiple aggregations in one query"""
        result = conn.execute(
            text(
                "SELECT COUNT(*) AS count, SUM(age) AS sum_age, AVG(age) AS avg_age FROM users"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row with multiple aggregations"

    def test_aggregation_with_where(self, conn):
        """Test aggregation with WHERE clause"""
        result = conn.execute(text("SELECT COUNT(*) FROM users WHERE age > 25"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row with COUNT(*) and WHERE"


class TestGQLAggregateOver:
    """Test AGGREGATE ... OVER (...) syntax"""

    def test_aggregate_count_over_subquery(self, conn):
        """Test AGGREGATE COUNT(*) OVER (SELECT ...)"""
        result = conn.execute(
            text("AGGREGATE COUNT(*) OVER (SELECT * FROM users WHERE age > 25)")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE COUNT(*) OVER"

    def test_aggregate_count_up_to_over_subquery(self, conn):
        """Test AGGREGATE COUNT_UP_TO(n) OVER (SELECT ...)"""
        result = conn.execute(
            text("AGGREGATE COUNT_UP_TO(5) OVER (SELECT * FROM users WHERE age > 20)")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE COUNT_UP_TO OVER"

    def test_aggregate_sum_over_subquery(self, conn):
        """Test AGGREGATE SUM(property) OVER (SELECT ...)"""
        result = conn.execute(
            text("AGGREGATE SUM(age) OVER (SELECT * FROM users WHERE age > 20)")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE SUM OVER"

    def test_aggregate_avg_over_subquery(self, conn):
        """Test AGGREGATE AVG(property) OVER (SELECT ...)"""
        result = conn.execute(
            text(
                "AGGREGATE AVG(age) OVER (SELECT * FROM users WHERE name != 'Unknown')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE AVG OVER"

    def test_aggregate_multiple_over_subquery(self, conn):
        """Test AGGREGATE with multiple functions OVER (SELECT ...)"""
        result = conn.execute(
            text(
                "AGGREGATE COUNT(*), SUM(age), AVG(age) OVER (SELECT * FROM users WHERE age >= 20)"
            )
        )
        data = result.all()
        assert len(data) == 1, (
            "Expected 1 row from AGGREGATE with multiple functions OVER"
        )

    def test_aggregate_with_alias_over_subquery(self, conn):
        """Test AGGREGATE ... AS alias OVER (SELECT ...)"""
        result = conn.execute(
            text(
                "AGGREGATE COUNT(*) AS total_count OVER (SELECT * FROM users WHERE age > 18)"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE with alias OVER"

    def test_aggregate_over_complex_subquery(self, conn):
        """Test AGGREGATE OVER complex subquery with multiple clauses"""
        # With ages 24, 30, 35: all 3 users have age > 20
        # COUNT of users with age > 20 = 3
        result = conn.execute(
            text(
                "AGGREGATE COUNT(*) OVER (SELECT * FROM users WHERE age > 20 LIMIT 10)"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE OVER complex subquery"
        assert data[0][0] == 1, f"Expected count of 1 users with age > 20, got {data[0][0]}"


class TestGQLComplexQueries:
    """Test complex queries combining multiple features"""

    def test_complex_select_with_all_clauses(self, conn):
        """Test SELECT with all possible clauses"""
        result = conn.execute(text("""
            SELECT DISTINCT ON (name) name, age, country
            FROM users
            WHERE age >= 16 AND country = 'Arland'
            ORDER BY age DESC, name ASC
            LIMIT 20
            OFFSET 0
        """))
        data = result.all()
        assert len(data) == 1, "Expected results from complex query"
        assert data[0][0] == "Elmerulia Frixell"

    def test_complex_where_with_synthetic_literals(self, conn):
        """Test WHERE with various synthetic literals"""
        ## TODO: query the 'Elmerulia Frixell's id first
        result = conn.execute(text("""
            SELECT * FROM users
            WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')
            AND create_time > DATETIME('2023-01-01T00:00:00Z')
        """))
        data = result.all()
        assert len(data) == 1, "Expected results from query with synthetic literals"
        # SELECT * columns sorted alphabetically: key=0, age=1, country=2,
        # create_time=3, description=4, name=5, settings=6, tags=7
        assert data[0][5] == "Elmerulia Frixell"

    def test_complex_aggregation_with_subquery(self, conn):
        """Test complex aggregation with subquery"""
        result = conn.execute(
            text("""
            AGGREGATE COUNT(*) AS active_users, AVG(age) AS avg_age
            OVER (
                SELECT DISTINCT name, age FROM users
                WHERE age > 18 AND name != 'Unknown'
                ORDER BY age DESC
                LIMIT 100
            )
        """)
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row from complex aggregation"

    def test_backward_comparator_queries(self, conn):
        """Test backward comparators (value operator property)"""
        # 25 < age means age > 25; only Travis (age=28) matches
        result = conn.execute(text("SELECT * FROM users WHERE 25 < age"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row (Travis, age=28) from backward comparator query"

        result = conn.execute(
            text("SELECT * FROM users WHERE 'Elmerulia Frixell' = name")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row matching backward equals for Elmerulia Frixell"
        # SELECT * columns sorted: key=0, age=1, country=2, create_time=3,
        # description=4, name=5, settings=6, tags=7
        assert data[0][5] == "Elmerulia Frixell"

    def test_fully_qualified_property_in_conditions(self, conn):
        """Test fully qualified properties in WHERE conditions"""
        # Travis has age=28 (>25) and name="Travis 'Ghost' Hayes"
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE users.age > 25 AND users.name = \"Travis 'Ghost' Hayes\""
            )
        )
        data = result.all()
        assert len(data) == 1, (
            "Expected 1 row (Travis) from fully qualified property conditions"
        )

    def test_nested_key_path_elements(self, conn):
        """Test key path elements (emulator-compatible single kind)"""
        # Nested key paths with multiple kinds not supported by test data
        # Test simple key query instead
        # TODO: query the correct 'Elmerulia Frixell's id first
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected results from nested key path query"


class TestGQLEdgeCases:
    """Test edge cases and special scenarios"""

    def test_empty_from_clause(self, conn):
        """Test SELECT without FROM clause"""
        result = conn.execute(text("SELECT COUNT(*)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from SELECT without FROM"

    def test_all_comparison_operators(self, conn):
        """Test all comparison operators against age=25 (ages: 14, 16, 28)"""
        # ages: Virginia=14, Elmerulia=16, Travis=28
        expected_counts = {
            "=": 0,   # no user has age 25
            "!=": 3,  # all 3 users
            "<": 2,   # Virginia(14), Elmerulia(16)
            "<=": 2,  # Virginia(14), Elmerulia(16)
            ">": 1,   # Travis(28)
            ">=": 1,  # Travis(28)
        }
        for op, expected in expected_counts.items():
            result = conn.execute(text(f"SELECT * FROM users WHERE age {op} 25"))
            data = result.all()
            assert len(data) == expected, f"Expected {expected} rows from query with {op} operator, got {len(data)}"

    def test_null_literal_conditions(self, conn):
        """Test NULL literal in conditions"""
        # All 3 users have settings=None, so IS NULL matches all of them
        result = conn.execute(text("SELECT * FROM users WHERE settings IS NULL"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows since all users have settings=None"

    def test_boolean_literal_conditions(self, conn):
        """Test boolean literals in conditions"""
        result = conn.execute(text("SELECT * FROM tasks WHERE is_done = true"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since no user has is_active property"

        result = conn.execute(text("SELECT * FROM tasks WHERE is_done = false"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows since no user has is_deleted property"

    def test_string_literal_with_quotes(self, conn):
        """Test string literals with various quote styles"""
        result = conn.execute(
            text("SELECT * FROM users WHERE name = 'Elmerulia Frixell'")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row matching Elmerulia Frixell (single-quoted)"

        result = conn.execute(
            text('SELECT * FROM users WHERE name = "Elmerulia Frixell"')
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row matching Elmerulia Frixell (double-quoted)"

    def test_integer_and_double_literals(self, conn):
        """Test integer and double literals"""
        # No user has age=30 (ages: 14, 16, 28)
        result = conn.execute(text("SELECT * FROM users WHERE age = 30"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since no user has age=30"

        # No user has a 'score' property
        result = conn.execute(text("SELECT * FROM users WHERE score = 95.5"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since no user has a score property"


class TestGQLKindlessQueries:
    """Test kindless queries (without FROM clause)"""

    def test_kindless_query_with_key_condition(self, conn):
        """Test query with __key__ condition (emulator needs FROM clause)"""
        # Emulator doesn't support kindless queries, use FROM clause
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected results from key condition query"

    def test_kindless_query_with_key_has_ancestor(self, conn):
        """Test query (emulator doesn't support kindless or HAS ANCESTOR)"""
        # Emulator doesn't support kindless queries or HAS ANCESTOR
        result = conn.execute(
            text("SELECT * FROM users WHERE __key__ = KEY(users, 'Virginia Robertson_id')")
        )
        data = result.all()
        assert len(data) == 1, "Expected results from key query"

    def test_kindless_aggregation(self, conn):
        """Test kindless aggregation query"""
        # Kindless COUNT(*) without FROM cannot query a specific kind,
        # so the dbapi returns 0
        result = conn.execute(text("SELECT COUNT(*)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from kindless COUNT(*)"
        assert data[0][0] == 0, "Expected 0 from kindless COUNT(*) (no kind specified)"


class TestGQLCaseInsensitivity:
    """Test case insensitivity of GQL keywords"""

    def test_select_case_insensitive(self, conn):
        """Test SELECT with different cases"""
        queries = [
            "SELECT * FROM users",
            "select * from users",
            "Select * From users",
            "sElEcT * fRoM users",
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 3, f"Expected 3 rows from query: {query}"

    def test_where_case_insensitive(self, conn):
        """Test WHERE with different cases"""
        # Only Travis (age=28) matches age > 25
        queries = [
            "SELECT * FROM users WHERE age > 25",
            "select * from users where age > 25",
            "SELECT * FROM users WhErE age > 25",
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 1, f"Expected 1 row (Travis, age=28) from query: {query}"

    def test_boolean_literals_case_insensitive(self, conn):
        """Test boolean literals with different cases"""
        # All tasks have is_done=False, so TRUE queries return 0, FALSE return 3
        true_queries = [
            "SELECT * FROM tasks WHERE is_done = TRUE",
            "SELECT * FROM tasks WHERE is_done = true",
            "SELECT * FROM tasks WHERE is_done = True",
        ]
        for query in true_queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 0, f"Expected 0 rows (all is_done=False): {query}"

        false_queries = [
            "SELECT * FROM tasks WHERE is_done = FALSE",
            "SELECT * FROM tasks WHERE is_done = false",
        ]
        for query in false_queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 3, f"Expected 3 rows (all is_done=False): {query}"

    def test_null_literal_case_insensitive(self, conn):
        """Test NULL literal with different cases"""
        queries = [
            "SELECT * FROM users WHERE settings = NULL",
            "SELECT * FROM users WHERE settings = null",
            "SELECT * FROM users WHERE settings = Null",
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 3, f"Expected 3 rows from query: {query}"

class TestGQLPropertyNaming:
    """Test property naming rules and edge cases"""
    def test_property_names_with_special_characters(self, conn):
        """Test property names with underscores, dollar signs, etc."""
        # These properties don't exist on user entities
        result = conn.execute(text("SELECT user_id, big$bux, __qux__ FROM users"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since user_id/big$bux/__qux__ properties don't exist"

    def test_backquoted_property_names(self, conn):
        """Test backquoted property names"""
        # These properties don't exist on user entities
        result = conn.execute(text("SELECT `first-name`, `x.y` FROM users"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since first-name/x.y properties don't exist"

    def test_escaped_backquotes_in_property_names(self, conn):
        """Test escaped backquotes in property names"""
        # This property doesn't exist on user entities
        result = conn.execute(text("SELECT `silly``putty` FROM users"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since silly`putty property doesn't exist"

    def test_fully_qualified_property_names_edge_case(self, conn):
        """Test fully qualified property names with kind prefix"""
        # Product kind doesn't exist in test dataset
        result = conn.execute(text("SELECT Product.Product.Name FROM Product"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since Product kind doesn't exist"


class TestGQLStringLiterals:
    """Test string literal formatting and escaping"""

    def test_single_quoted_strings(self, conn):
        """Test single-quoted string literals"""
        result = conn.execute(
            text("SELECT name FROM users WHERE name = 'Elmerulia Frixell'")
        )
        data = result.all()
        assert len(data) == 1, "Expected results from single-quoted string"
        assert data[0][0] == "Elmerulia Frixell"

    def test_double_quoted_strings(self, conn):
        """Test double-quoted string literals"""
        result = conn.execute(
            text('SELECT name FROM users WHERE name = "Elmerulia Frixell"')
        )
        data = result.all()
        assert len(data) == 1, "Expected results from double-quoted string"
        assert data[0][0] == "Elmerulia Frixell"

    def test_escaped_quotes_in_strings(self, conn):
        """Test string literals (emulator may not support all escape styles)"""
        # Escaped quotes syntax varies - test basic string matching
        result = conn.execute(
            text("SELECT name FROM users WHERE name = \"Travis 'Ghost' Hayes\"")
        )
        data = result.all()
        assert len(data) == 1, "Expected results from string condition"
        assert data[0][0] == "Travis 'Ghost' Hayes"


class TestGQLNumericLiterals:
    """Test numeric literal formats"""

    def test_integer_literals(self, conn):
        """Test various integer literal formats"""
        # User ages are 14, 16, 28 - none match these test values
        integer_tests = [
            ("0", 0),
            ("11", 11),
            ("+5831", 5831),
            ("-37", -37),
            ("3827438927", 3827438927),
        ]
        for literal, _expected in integer_tests:
            result = conn.execute(text(f"SELECT * FROM users WHERE age = {literal}"))
            data = result.all()
            assert len(data) == 0, f"Expected 0 rows since no user has age={literal}"

    def test_double_literals(self, conn):
        """Test various double literal formats"""
        # No user entity has a 'score' property
        double_tests = [
            "0.0",
            "+58.31",
            "-37.0",
            "3827438927.0",
            "-3.",
            "+.1",
            "314159e-5",
            "6.022E23",
        ]
        for literal in double_tests:
            result = conn.execute(text(f"SELECT * FROM users WHERE score = {literal}"))
            data = result.all()
            assert len(data) == 0, f"Expected 0 rows since no user has a score property (literal: {literal})"

    def test_integer_vs_double_inequality(self, conn):
        """Test that integer is not equal to double in Datastore type system"""
        # tasks have hours as integer: task1=1, task2=2, task3=3
        # Datastore distinguishes integer 2 from double 2.0
        result = conn.execute(text("SELECT * FROM tasks WHERE hours = 2.0"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows (integer 2 != double 2.0 in Datastore)"

        # Integer comparison: hours > 2 matches task3 (hours=3)
        result = conn.execute(text("SELECT * FROM tasks WHERE hours > 2"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row (task3 with hours=3)"


class TestGQLDateTimeLiterals:
    """Test DATETIME literal formats"""

    def test_datetime_basic_format(self, conn):
        """Test basic DATETIME format with microseconds"""
        # conftest stores create_time=datetime(2025,1,1,1,2,3,4) = 4 microseconds
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE create_time = DATETIME('2025-01-01T01:02:03.000004Z')"
            )
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 users with create_time matching 4μs (user1 and user2)"

    def test_datetime_with_timezone_offset(self, conn):
        """Test DATETIME with timezone offset - falls back to client-side filtering"""
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE create_time = DATETIME('2025-01-01T01:02:03.00004+00:00')"
            )
        )
        data = result.all()
        # 40μs doesn't match any user (user1/user2 have 4μs), so 0 results
        assert len(data) == 0

    def test_datetime_microseconds(self, conn):
        """Test DATETIME with microseconds - falls back to client-side filtering"""
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE create_time = DATETIME('2025-01-01T01:02:03.4+00:00')"
            )
        )
        data = result.all()
        # 400000μs doesn't match any user (user1/user2 have 4μs), so 0 results
        assert len(data) == 0

    def test_datetime_without_microseconds(self, conn):
        """Test DATETIME without microseconds - falls back to client-side filtering"""
        result = conn.execute(
            text("SELECT * FROM users WHERE create_time = DATETIME('2025-01-01T01:02:03+00:00')")
        )
        data = result.all()
        # 0μs doesn't match any user (user1/user2 have 4μs), so 0 results
        assert len(data) == 0


class TestGQLOperatorBehavior:
    """Test special operator behaviors"""

    def test_equals_as_contains_for_multivalued_properties(self, conn):
        """Test = operator functioning as CONTAINS for multi-valued properties"""
        # This should work like CONTAINS for multi-valued properties
        result = conn.execute(
            text("SELECT * FROM Task WHERE tags = 'House' OR tags = 'Wild'")
        )
        data = result.all()
        assert len(data) == 0, (
            "Expected results from = operator on multi-valued property"
        )

    def test_equals_as_in_operator(self, conn):
        """Test = operator functioning as IN operator"""
        # value = property is same as value IN property
        result = conn.execute(
            text("SELECT * FROM users WHERE name IN ARRAY('Elmerulia Frixell')")
        )
        data = result.all()
        assert len(data) == 1, "Expected results from = operator as IN"

    def test_is_null_equivalent_to_equals_null(self, conn):
        """Test IS NULL equivalent to = NULL"""
        result1 = conn.execute(text("SELECT * FROM users WHERE email IS NULL"))
        data1 = result1.all()

        result2 = conn.execute(text("SELECT * FROM users WHERE email = NULL"))
        data2 = result2.all()

        assert len(data1) == len(data2), "IS NULL and = NULL should return same results"

    def test_null_as_explicit_value(self, conn):
        """Test NULL as explicit value, not absence of value"""
        # No user entity has a 'nonexistent' property
        result = conn.execute(text("SELECT * FROM users WHERE nonexistent = NULL"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since no user has nonexistent property"


class TestGQLLimitOffsetAdvanced:
    """Test advanced LIMIT and OFFSET behaviors"""

    def test_limit_with_cursor_and_integer(self, conn):
        """Test LIMIT with integer (emulator doesn't support cursor syntax)"""
        # @cursor syntax not supported by emulator, use plain LIMIT
        # LIMIT 5 on 3 users returns all 3 (fewer than limit)
        result = conn.execute(text("SELECT * FROM users LIMIT 5"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows from LIMIT 5 (only 3 users exist)"

    def test_offset_with_cursor_and_integer(self, conn):
        """Test OFFSET with integer (emulator doesn't support cursor syntax)"""
        # @cursor syntax not supported by emulator, use plain OFFSET
        # result = conn.execute(text("SELECT * FROM users OFFSET @cursor, 2"))
        pass

    def test_offset_plus_notation(self, conn):
        """Test OFFSET (emulator doesn't support arithmetic/cursor)"""
        # @cursor + number syntax not supported by emulator
        # result = conn.execute(text("SELECT * FROM users OFFSET @cursor + 1"))
        pass

    def test_offset_without_limit(self, conn):
        """Test OFFSET without LIMIT"""
        # OFFSET 1 on 3 users skips 1, returns 2
        result = conn.execute(text("SELECT * FROM users OFFSET 1"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows from OFFSET 1 (3 users minus 1 skipped)"

class TestGQLProjectionQueries:
    """Test projection query behaviors"""

    def test_projection_query_duplicates(self, conn):
        """Test that projection queries may contain duplicates"""
        result = conn.execute(text("SELECT tag FROM tasks ORDER BY tag DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows from projection query (one per task)"
        assert data[0][0] == 'Wild', "Expected Wild tag"
        assert data[1][0] == 'House', "Expected House tag"
        assert data[2][0] == 'Apartment', "Expected Apartment tag"

    def test_distinct_projection_query(self, conn):
        """Test DISTINCT with projection query"""
        # 3 distinct tags: "House", "Wild", "Apartment"
        result = conn.execute(text("SELECT DISTINCT tag FROM tasks ORDER BY tag DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 distinct tag values"
        assert data[0][0] == 'Wild', "Expected Wild tag"
        assert data[1][0] == 'House', "Expected House tag"
        assert data[2][0] == 'Apartment', "Expected Apartment tag"

    def test_distinct_on_projection_query(self, conn):
        """Test DISTINCT ON with projection query"""
        # No task entity has a 'category' property
        result = conn.execute(
            text("SELECT DISTINCT ON (category) category, tag FROM tasks")
        )
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since category property doesn't exist on tasks"

    def test_distinct_vs_distinct_on_equivalence(self, conn):
        """Test that DISTINCT a,b,c is identical to DISTINCT ON (a,b,c) a,b,c"""
        result1 = conn.execute(text("SELECT DISTINCT name, age FROM users"))
        data1 = result1.all()
        assert len(data1) == 3, "Expected 3 distinct (name, age) combinations"

        result2 = conn.execute(
            text("SELECT DISTINCT ON (name, age) name, age FROM users")
        )
        data2 = result2.all()
        assert len(data2) == 3, "Expected 3 distinct (name, age) combinations from DISTINCT ON"

        assert len(data1) == len(data2), (
            "DISTINCT and DISTINCT ON should return same results"
        )


class TestGQLOrderByRestrictions:
    """Test ORDER BY restrictions with inequality operators"""

    def test_inequality_with_order_by_first_property(self, conn):
        """Test inequality operator with ORDER BY - property must be first"""
        # Only Travis (age=28) matches age > 25
        result = conn.execute(
            text("SELECT * FROM users WHERE age > 25 ORDER BY age, name")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 row (Travis, age=28) from age > 25 with ORDER BY"
        assert "Travis 'Ghost' Hayes" in data[0], "Excepted results should be 1"

    def test_multiple_properties_order_by(self, conn):
        """Test ORDER BY with multiple properties"""
        result = conn.execute(
            text("SELECT * FROM users ORDER BY age DESC, name ASC, country")
        )
        data = result.all()
        assert len(data) == 3, "Expected 3 rows from ORDER BY with multiple properties"

class TestGQLAncestorQueries:
    """Test ancestor relationship queries"""
    """
    Not support yet
    """
    pass

class TestGQLComplexKeyPaths:
    """Test complex key path elements"""

    def test_nested_key_path_with_project_namespace(self, conn):
        """Test key path (emulator doesn't support PROJECT/NAMESPACE)"""
        # PROJECT and NAMESPACE not supported by emulator
        # Test basic key query instead
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE __key__ = KEY(users, 'Elmerulia Frixell_id')"
            )
        )
        data = result.all()
        assert len(data) == 1, "Expected results from key path query"

    def test_key_path_with_integer_ids(self, conn):
        """Test key path (emulator needs FROM clause)"""
        # Emulator requires FROM clause and doesn't support kindless queries
        result = conn.execute(
            text("SELECT * FROM users WHERE __key__ = KEY(users, 'Virginia Robertson_id')")
        )
        data = result.all()
        assert len(data) == 1, "Expected results from key path query"


class TestGQLBlobLiterals:
    """Test BLOB literal functionality"""

    def test_blob_literal_basic(self, conn):
        """Test basic BLOB literal"""
        result = conn.execute(
            text("SELECT * FROM tasks WHERE encrypted_formula = BLOB('\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\xda\xed\xc1\x01\x01\x00\x00\x00\xc2\xa0\xf7Om\x00\x00\x00\x00IEND\xaeB`\x82')")
        )
        data = result.all()
        assert len(data) == 1, "Expected 1 task matching BLOB literal (task1 has PNG bytes)"

    def test_blob_literal_in_conditions(self, conn):
        """Test BLOB literal in various conditions"""
        result = conn.execute(
            text("SELECT * FROM tasks WHERE encrypted_formula != BLOB('\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\xda\xed\xc1\x01\x01\x00\x00\x00\xc2\xa0\xf7Om\x00\x00\x00\x00IEND\xaeB`\x82')")
        )
        data = result.all()
        assert len(data) == 2, "Expected 2 tasks with different encrypted_formula (task2 and task3)"


class TestGQLOperatorPrecedence:
    """Test operator precedence (AND has higher precedence than OR)"""

    def test_and_or_precedence(self, conn):
        """Test AND has higher precedence than OR"""
        # a OR b AND c should parse as a OR (b AND c)
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE name = 'Elmerulia Frixell' OR name > 15 AND country= 'Arland'"
            )
        )
        data = result.all()
        assert len(data[0][0]) == 1, "Expected results from AND/OR precedence test"

    def test_parentheses_override_precedence(self, conn):
        """Test parentheses can override precedence"""
        # (a OR b) AND c
        result = conn.execute(
            text(
                "SELECT * FROM users WHERE (name = 'Elmerulia Frixell' OR name = 'Virginia Robertson') AND age > 10"
            )
        )
        data = result.all()
        assert len(data) == 2, "Expected results from parentheses precedence override"


class TestGQLPerformanceOptimizations:
    """Test performance-related query patterns"""

    def test_key_only_query_performance(self, conn):
        """Test __key__ only query (should be faster)"""
        result = conn.execute(text("SELECT __key__ FROM users WHERE age > 25"))
        data = result.all()
        assert len(data) == 1, "Expected results from key-only query"

    def test_projection_query_performance(self, conn):
        """Test projection query (should be faster than SELECT *)"""
        result = conn.execute(text("SELECT name, age FROM users WHERE country = 'Arland'"))
        data = result.all()
        assert len(data) == 1, "Expected results from projection query"


class TestGQLErrorCases:
    """Test edge cases and potential error conditions"""

    def test_property_name_case_sensitivity(self, conn):
        """Test that property names are case sensitive"""
        # Name and NAME don't exist on users (only lowercase 'name' does)
        result = conn.execute(text("SELECT Name, name, NAME FROM users"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows (projection on non-existent Name/NAME returns nothing)"

    def test_kind_name_case_sensitivity(self, conn):
        """Test that kind names are case sensitive"""
        # 'Users' (capital U) is different from 'users' (lowercase) in Datastore
        result = conn.execute(text("SELECT * FROM Users"))
        data = result.all()
        assert len(data) == 0, "Expected 0 rows since 'Users' kind doesn't exist (only 'users')"
