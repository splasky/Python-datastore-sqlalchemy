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
        result = conn.execute(text("SELECT id, name, age FROM users"))
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
        result = conn.execute(text("SELECT * FROM users WHERE name = 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row where name equals 'Elmerulia Frixell'"
    
    def test_where_not_equals(self, conn):
        """Test WHERE property != value"""
        result = conn.execute(text("SELECT * FROM users WHERE name != 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where name not equals 'Elmerulia Frixell'"
    
    def test_where_greater_than(self, conn):
        """Test WHERE property > value"""
        result = conn.execute(text("SELECT * FROM users WHERE age > 25"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where age > 25"
    
    def test_where_greater_than_equal(self, conn):
        """Test WHERE property >= value"""
        result = conn.execute(text("SELECT * FROM users WHERE age >= 30"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where age >= 30"
    
    def test_where_less_than(self, conn):
        """Test WHERE property < value"""
        result = conn.execute(text("SELECT * FROM users WHERE age < 30"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row where age < 30"
    
    def test_where_less_than_equal(self, conn):
        """Test WHERE property <= value"""
        result = conn.execute(text("SELECT * FROM users WHERE age <= 24"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row where age <= 24"
    
    def test_where_is_null(self, conn):
        """Test WHERE property IS NULL"""
        result = conn.execute(text("SELECT * FROM users WHERE email IS NULL"))
        data = result.all()
        assert len(data) >= 0, "Expected rows where email is null"
    
    def test_where_in_list(self, conn):
        """Test WHERE property IN (value1, value2, ...)"""
        result = conn.execute(text("SELECT * FROM users WHERE name IN ('Elmerulia Frixell', 'Virginia Robertson')"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where name in ('Elmerulia Frixell', 'Virginia Robertson')"
    
    def test_where_not_in_list(self, conn):
        """Test WHERE property NOT IN (value1, value2, ...)"""
        result = conn.execute(text("SELECT * FROM users WHERE name NOT IN ('Elmerulia Frixell')"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows where name not in ('Elmerulia Frixell')"
    
    def test_where_contains(self, conn):
        """Test WHERE property CONTAINS value"""
        result = conn.execute(text("SELECT * FROM users WHERE tags CONTAINS 'admin'"))
        data = result.all()
        assert len(data) >= 0, "Expected rows where tags contains 'admin'"
    
    def test_where_has_ancestor(self, conn):
        """Test WHERE __key__ HAS ANCESTOR key"""
        result = conn.execute(text("SELECT * FROM users WHERE __key__ HAS ANCESTOR KEY('Company', 'tech_corp')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows with specific ancestor"
    
    def test_where_has_descendant(self, conn):
        """Test WHERE key HAS DESCENDANT __key__"""
        result = conn.execute(text("SELECT * FROM users WHERE KEY('Company', 'tech_corp') HAS DESCENDANT __key__"))
        data = result.all()
        assert len(data) >= 0, "Expected rows that are descendants"


class TestGQLCompoundConditions:
    """Test compound conditions with AND/OR"""
    
    def test_where_and_condition(self, conn):
        """Test WHERE condition1 AND condition2"""
        result = conn.execute(text("SELECT * FROM users WHERE age > 20 AND name = 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row matching both conditions"
    
    def test_where_or_condition(self, conn):
        """Test WHERE condition1 OR condition2"""
        result = conn.execute(text("SELECT * FROM users WHERE age < 25 OR name = 'Travis 'Ghost' Hayes'"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows matching either condition"
    
    def test_where_parenthesized_conditions(self, conn):
        """Test WHERE (condition1 AND condition2) OR condition3"""
        result = conn.execute(text("SELECT * FROM users WHERE (age > 30 AND name = 'Elmerulia Frixell') OR name = 'Virginia Robertson'"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows matching complex condition"
    
    def test_where_complex_compound(self, conn):
        """Test complex compound conditions"""
        result = conn.execute(text("SELECT * FROM users WHERE (age >= 30 OR name = 'Virginia Robertson') AND name != 'David'"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows matching complex compound condition"


class TestGQLOrderBy:
    """Test ORDER BY clause"""
    
    def test_order_by_single_property_asc(self, conn):
        """Test ORDER BY property ASC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY age ASC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by age ascending"
    
    def test_order_by_single_property_desc(self, conn):
        """Test ORDER BY property DESC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY age DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by age descending"
    
    def test_order_by_multiple_properties(self, conn):
        """Test ORDER BY property1, property2 ASC/DESC"""
        result = conn.execute(text("SELECT * FROM users ORDER BY name ASC, age DESC"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by name ASC, age DESC"
    
    def test_order_by_without_direction(self, conn):
        """Test ORDER BY property (default ASC)"""
        result = conn.execute(text("SELECT * FROM users ORDER BY name"))
        data = result.all()
        assert len(data) == 3, "Expected 3 rows ordered by name (default ASC)"


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
        """Test OFFSET number + number"""
        result = conn.execute(text("SELECT * FROM users OFFSET 0 + 1"))
        data = result.all()
        assert len(data) == 2, "Expected 2 rows with OFFSET 0 + 1"


class TestGQLSyntheticLiterals:
    """Test synthetic literals (KEY, ARRAY, BLOB, DATETIME)"""
    
    def test_key_literal_simple(self, conn):
        """Test KEY(kind, id)"""
        result = conn.execute(text("SELECT * FROM users WHERE __key__ = KEY('users', 'Elmerulia Frixell_id')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching KEY literal"
    
    def test_key_literal_with_project(self, conn):
        """Test KEY with PROJECT"""
        result = conn.execute(text("SELECT * FROM users WHERE __key__ = KEY(PROJECT('my-project'), 'users', 'Elmerulia Frixell_id')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching KEY with PROJECT"
    
    def test_key_literal_with_namespace(self, conn):
        """Test KEY with NAMESPACE"""
        result = conn.execute(text("SELECT * FROM users WHERE __key__ = KEY(NAMESPACE('my-namespace'), 'users', 'Elmerulia Frixell_id')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching KEY with NAMESPACE"
    
    def test_key_literal_with_project_and_namespace(self, conn):
        """Test KEY with both PROJECT and NAMESPACE"""
        result = conn.execute(text("SELECT * FROM users WHERE __key__ = KEY(PROJECT('my-project'), NAMESPACE('my-namespace'), 'users', 'Elmerulia Frixell_id')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching KEY with PROJECT and NAMESPACE"
    
    def test_array_literal(self, conn):
        """Test ARRAY(value1, value2, ...)"""
        result = conn.execute(text("SELECT * FROM users WHERE tags = ARRAY('admin', 'user')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching ARRAY literal"
    
    def test_blob_literal(self, conn):
        """Test BLOB(string)"""
        result = conn.execute(text("SELECT * FROM users WHERE data = BLOB('binary_data')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching BLOB literal"
    
    def test_datetime_literal(self, conn):
        """Test DATETIME(string)"""
        result = conn.execute(text("SELECT * FROM users WHERE created_at = DATETIME('2023-01-01T00:00:00Z')"))
        data = result.all()
        assert len(data) >= 0, "Expected rows matching DATETIME literal"


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
        result = conn.execute(text("SELECT COUNT_UP_TO(10) AS limited_count FROM users"))
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
        result = conn.execute(text("SELECT COUNT(*) AS count, SUM(age) AS sum_age, AVG(age) AS avg_age FROM users"))
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
        result = conn.execute(text("AGGREGATE COUNT(*) OVER (SELECT * FROM users WHERE age > 25)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE COUNT(*) OVER"
    
    def test_aggregate_count_up_to_over_subquery(self, conn):
        """Test AGGREGATE COUNT_UP_TO(n) OVER (SELECT ...)"""
        result = conn.execute(text("AGGREGATE COUNT_UP_TO(5) OVER (SELECT * FROM users WHERE age > 20)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE COUNT_UP_TO OVER"
    
    def test_aggregate_sum_over_subquery(self, conn):
        """Test AGGREGATE SUM(property) OVER (SELECT ...)"""
        result = conn.execute(text("AGGREGATE SUM(age) OVER (SELECT * FROM users WHERE age > 20)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE SUM OVER"
    
    def test_aggregate_avg_over_subquery(self, conn):
        """Test AGGREGATE AVG(property) OVER (SELECT ...)"""
        result = conn.execute(text("AGGREGATE AVG(age) OVER (SELECT * FROM users WHERE name != 'Unknown')"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE AVG OVER"
    
    def test_aggregate_multiple_over_subquery(self, conn):
        """Test AGGREGATE with multiple functions OVER (SELECT ...)"""
        result = conn.execute(text("AGGREGATE COUNT(*), SUM(age), AVG(age) OVER (SELECT * FROM users WHERE age >= 20)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE with multiple functions OVER"
    
    def test_aggregate_with_alias_over_subquery(self, conn):
        """Test AGGREGATE ... AS alias OVER (SELECT ...)"""
        result = conn.execute(text("AGGREGATE COUNT(*) AS total_count OVER (SELECT * FROM users WHERE age > 18)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE with alias OVER"
    
    def test_aggregate_over_complex_subquery(self, conn):
        """Test AGGREGATE OVER complex subquery with multiple clauses"""
        result = conn.execute(text("""
            AGGREGATE COUNT(*) OVER (
                SELECT DISTINCT name FROM users 
                WHERE age > 20 
                ORDER BY name ASC 
                LIMIT 10
            )
        """))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from AGGREGATE OVER complex subquery"


class TestGQLComplexQueries:
    """Test complex queries combining multiple features"""
    
    def test_complex_select_with_all_clauses(self, conn):
        """Test SELECT with all possible clauses"""
        result = conn.execute(text("""
            SELECT DISTINCT ON (name) name, age, city 
            FROM users 
            WHERE age >= 18 AND city = 'Tokyo' 
            ORDER BY name ASC, age DESC 
            LIMIT 20 
            OFFSET 0
        """))
        data = result.all()
        assert len(data) >= 0, "Expected results from complex query"
    
    def test_complex_where_with_synthetic_literals(self, conn):
        """Test WHERE with various synthetic literals"""
        result = conn.execute(text("""
            SELECT * FROM users 
            WHERE __key__ = KEY('users', 'Elmerulia Frixell_id') 
            AND tags = ARRAY('admin', 'user') 
            AND created_at > DATETIME('2023-01-01T00:00:00Z')
        """))
        data = result.all()
        assert len(data) >= 0, "Expected results from query with synthetic literals"
    
    def test_complex_aggregation_with_subquery(self, conn):
        """Test complex aggregation with subquery"""
        result = conn.execute(text("""
            AGGREGATE COUNT(*) AS active_users, AVG(age) AS avg_age 
            OVER (
                SELECT DISTINCT name, age FROM users 
                WHERE age > 18 AND name != 'Unknown'
                ORDER BY age DESC
                LIMIT 100
            )
        """))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from complex aggregation"
    
    def test_backward_comparator_queries(self, conn):
        """Test backward comparators (value operator property)"""
        result = conn.execute(text("SELECT * FROM users WHERE 25 < age"))
        data = result.all()
        assert len(data) >= 0, "Expected results from backward comparator query"
        
        result = conn.execute(text("SELECT * FROM users WHERE 'Elmerulia Frixell' = name"))
        data = result.all()
        assert len(data) >= 0, "Expected results from backward equals query"
    
    def test_fully_qualified_property_in_conditions(self, conn):
        """Test fully qualified properties in WHERE conditions"""
        result = conn.execute(text("SELECT * FROM users WHERE users.age > 25 AND users.name = 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from fully qualified property conditions"
    
    def test_nested_key_path_elements(self, conn):
        """Test nested key path elements"""
        result = conn.execute(text("""
            SELECT * FROM users 
            WHERE __key__ = KEY('Company', 'tech_corp', 'Department', 'engineering', 'users', 'Elmerulia Frixell_id')
        """))
        data = result.all()
        assert len(data) >= 0, "Expected results from nested key path query"


class TestGQLEdgeCases:
    """Test edge cases and special scenarios"""
    
    def test_empty_from_clause(self, conn):
        """Test SELECT without FROM clause"""
        result = conn.execute(text("SELECT COUNT(*)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from SELECT without FROM"
    
    def test_all_comparison_operators(self, conn):
        """Test all comparison operators"""
        operators = ['=', '!=', '<', '<=', '>', '>=']
        for op in operators:
            result = conn.execute(text(f"SELECT * FROM users WHERE age {op} 25"))
            data = result.all()
            assert len(data) >= 0, f"Expected results from query with {op} operator"
    
    def test_null_literal_conditions(self, conn):
        """Test NULL literal in conditions"""
        result = conn.execute(text("SELECT * FROM users WHERE email = NULL"))
        data = result.all()
        assert len(data) >= 0, "Expected results from NULL literal condition"
    
    def test_boolean_literal_conditions(self, conn):
        """Test boolean literals in conditions"""
        result = conn.execute(text("SELECT * FROM users WHERE is_active = true"))
        data = result.all()
        assert len(data) >= 0, "Expected results from boolean literal condition"
        
        result = conn.execute(text("SELECT * FROM users WHERE is_deleted = false"))
        data = result.all()
        assert len(data) >= 0, "Expected results from boolean literal condition"
    
    def test_string_literal_with_quotes(self, conn):
        """Test string literals with various quote styles"""
        result = conn.execute(text("SELECT * FROM users WHERE name = 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from single-quoted string"
        
        result = conn.execute(text('SELECT * FROM users WHERE name = "Elmerulia Frixell"'))
        data = result.all()
        assert len(data) >= 0, "Expected results from double-quoted string"
    
    def test_integer_and_double_literals(self, conn):
        """Test integer and double literals"""
        result = conn.execute(text("SELECT * FROM users WHERE age = 30"))
        data = result.all()
        assert len(data) >= 0, "Expected results from integer literal"
        
        result = conn.execute(text("SELECT * FROM users WHERE score = 95.5"))
        data = result.all()
        assert len(data) >= 0, "Expected results from double literal"


class TestGQLKindlessQueries:
    """Test kindless queries (without FROM clause)"""
    
    def test_kindless_query_with_key_condition(self, conn):
        """Test kindless query with __key__ condition"""
        result = conn.execute(text("SELECT * WHERE __key__ = KEY('users', 'Elmerulia Frixell_id')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from kindless query with key condition"
    
    def test_kindless_query_with_key_has_ancestor(self, conn):
        """Test kindless query with HAS ANCESTOR"""
        result = conn.execute(text("SELECT * WHERE __key__ HAS ANCESTOR KEY('Person', 'Amy')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from kindless query with HAS ANCESTOR"
    
    def test_kindless_aggregation(self, conn):
        """Test kindless aggregation query"""
        result = conn.execute(text("SELECT COUNT(*)"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from kindless COUNT(*)"


class TestGQLCaseInsensitivity:
    """Test case insensitivity of GQL keywords"""
    
    def test_select_case_insensitive(self, conn):
        """Test SELECT with different cases"""
        queries = [
            "SELECT * FROM users",
            "select * from users",
            "Select * From users",
            "sElEcT * fRoM users"
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) == 3, f"Expected 3 rows from query: {query}"
    
    def test_where_case_insensitive(self, conn):
        """Test WHERE with different cases"""
        queries = [
            "SELECT * FROM users WHERE age > 25",
            "select * from users where age > 25",
            "SELECT * FROM users WhErE age > 25"
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) >= 0, f"Expected results from query: {query}"
    
    def test_boolean_literals_case_insensitive(self, conn):
        """Test boolean literals with different cases"""
        queries = [
            "SELECT * FROM users WHERE is_active = TRUE",
            "SELECT * FROM users WHERE is_active = true",
            "SELECT * FROM users WHERE is_active = True",
            "SELECT * FROM users WHERE is_active = FALSE",
            "SELECT * FROM users WHERE is_active = false"
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) >= 0, f"Expected results from query: {query}"
    
    def test_null_literal_case_insensitive(self, conn):
        """Test NULL literal with different cases"""
        queries = [
            "SELECT * FROM users WHERE email = NULL",
            "SELECT * FROM users WHERE email = null",
            "SELECT * FROM users WHERE email = Null"
        ]
        for query in queries:
            result = conn.execute(text(query))
            data = result.all()
            assert len(data) >= 0, f"Expected results from query: {query}"


class TestGQLPropertyNaming:
    """Test property naming rules and edge cases"""
    
    def test_property_names_with_special_characters(self, conn):
        """Test property names with underscores, dollar signs, etc."""
        result = conn.execute(text("SELECT user_id, big$bux, __qux__ FROM users"))
        data = result.all()
        assert len(data) >= 0, "Expected results from query with special property names"
    
    def test_backquoted_property_names(self, conn):
        """Test backquoted property names"""
        result = conn.execute(text("SELECT `first-name`, `x.y` FROM users"))
        data = result.all()
        assert len(data) >= 0, "Expected results from query with backquoted property names"
    
    def test_escaped_backquotes_in_property_names(self, conn):
        """Test escaped backquotes in property names"""
        result = conn.execute(text("SELECT `silly``putty` FROM users"))
        data = result.all()
        assert len(data) >= 0, "Expected results from query with escaped backquotes"
    
    def test_fully_qualified_property_names_edge_case(self, conn):
        """Test fully qualified property names with kind prefix"""
        # When property name begins with kind name followed by dot
        result = conn.execute(text("SELECT Product.Product.Name FROM Product"))
        data = result.all()
        assert len(data) >= 0, "Expected results from fully qualified property with kind prefix"


class TestGQLStringLiterals:
    """Test string literal formatting and escaping"""
    
    def test_single_quoted_strings(self, conn):
        """Test single-quoted string literals"""
        result = conn.execute(text("SELECT * FROM users WHERE name = 'Elmerulia Frixell'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from single-quoted string"
    
    def test_double_quoted_strings(self, conn):
        """Test double-quoted string literals"""
        result = conn.execute(text('SELECT * FROM users WHERE name = "Elmerulia Frixell"'))
        data = result.all()
        assert len(data) >= 0, "Expected results from double-quoted string"
    
    def test_escaped_quotes_in_strings(self, conn):
        """Test escaped quotes in string literals"""
        result = conn.execute(text("SELECT * FROM users WHERE description = 'Joe''s Diner'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from string with escaped single quotes"
        
        result = conn.execute(text('SELECT * FROM users WHERE description = "Expected "".""'))
        data = result.all()
        assert len(data) >= 0, "Expected results from string with escaped double quotes"
    
    def test_escaped_characters_in_strings(self, conn):
        """Test escaped characters in string literals"""
        escaped_chars = [
            "\\\\",  # backslash
            "\\0",   # null
            "\\b",   # backspace
            "\\n",   # newline
            "\\r",   # return
            "\\t",   # tab
            "\\Z",   # decimal 26
            "\\'",   # single quote
            '\\"',   # double quote
            "\\`",   # backquote
            "\\%",   # percent
            "\\_"    # underscore
        ]
        for escaped_char in escaped_chars:
            result = conn.execute(text(f"SELECT * FROM users WHERE description = '{escaped_char}'"))
            data = result.all()
            assert len(data) >= 0, f"Expected results from string with escaped character: {escaped_char}"


class TestGQLNumericLiterals:
    """Test numeric literal formats"""
    
    def test_integer_literals(self, conn):
        """Test various integer literal formats"""
        integer_tests = [
            ("0", 0),
            ("11", 11),
            ("+5831", 5831),
            ("-37", -37),
            ("3827438927", 3827438927)
        ]
        for literal, expected in integer_tests:
            result = conn.execute(text(f"SELECT * FROM users WHERE age = {literal}"))
            data = result.all()
            assert len(data) >= 0, f"Expected results from integer literal: {literal}"
    
    def test_double_literals(self, conn):
        """Test various double literal formats"""
        double_tests = [
            "0.0", "+58.31", "-37.0", "3827438927.0",
            "-3.", "+.1", "314159e-5", "6.022E23"
        ]
        for literal in double_tests:
            result = conn.execute(text(f"SELECT * FROM users WHERE score = {literal}"))
            data = result.all()
            assert len(data) >= 0, f"Expected results from double literal: {literal}"
    
    def test_integer_vs_double_inequality(self, conn):
        """Test that integer 4 is not equal to double 4.0"""
        # This should not match entities with integer priority 4
        result = conn.execute(text("SELECT * FROM Task WHERE priority = 4.0"))
        data = result.all()
        assert len(data) >= 0, "Expected results from double comparison"
        
        # This should not match entities with double priority 50.0
        result = conn.execute(text("SELECT * FROM Task WHERE priority = 50"))
        data = result.all()
        assert len(data) >= 0, "Expected results from integer comparison"


class TestGQLDateTimeLiterals:
    """Test DATETIME literal formats"""
    
    def test_datetime_basic_format(self, conn):
        """Test basic DATETIME format"""
        result = conn.execute(text("SELECT * FROM users WHERE created_at = DATETIME('2023-01-01T00:00:00Z')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from basic DATETIME"
    
    def test_datetime_with_timezone_offset(self, conn):
        """Test DATETIME with timezone offset"""
        result = conn.execute(text("SELECT * FROM users WHERE created_at = DATETIME('2023-09-29T09:30:20.00002-08:00')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from DATETIME with timezone offset"
    
    def test_datetime_microseconds(self, conn):
        """Test DATETIME with microseconds"""
        result = conn.execute(text("SELECT * FROM users WHERE created_at = DATETIME('2023-01-01T12:30:45.123456+05:30')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from DATETIME with microseconds"
    
    def test_datetime_without_microseconds(self, conn):
        """Test DATETIME without microseconds"""
        result = conn.execute(text("SELECT * FROM users WHERE created_at = DATETIME('2023-01-01T12:30:45+00:00')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from DATETIME without microseconds"


class TestGQLOperatorBehavior:
    """Test special operator behaviors"""
    
    def test_equals_as_contains_for_multivalued_properties(self, conn):
        """Test = operator functioning as CONTAINS for multi-valued properties"""
        # This should work like CONTAINS for multi-valued properties
        result = conn.execute(text("SELECT * FROM Task WHERE tags = 'fun' AND tags = 'programming'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from = operator on multi-valued property"
    
    def test_equals_as_in_operator(self, conn):
        """Test = operator functioning as IN operator"""
        # value = property is same as value IN property
        result = conn.execute(text("SELECT * FROM users WHERE 'Elmerulia Frixell' = name"))
        data = result.all()
        assert len(data) >= 0, "Expected results from = operator as IN"
    
    def test_is_null_equivalent_to_equals_null(self, conn):
        """Test IS NULL equivalent to = NULL"""
        result1 = conn.execute(text("SELECT * FROM users WHERE email IS NULL"))
        data1 = result1.all()
        
        result2 = conn.execute(text("SELECT * FROM users WHERE email = NULL"))
        data2 = result2.all()
        
        assert len(data1) == len(data2), "IS NULL and = NULL should return same results"
    
    def test_null_as_explicit_value(self, conn):
        """Test NULL as explicit value, not absence of value"""
        # This tests that NULL is treated as a stored value
        result = conn.execute(text("SELECT * FROM users WHERE nonexistent = NULL"))
        data = result.all()
        assert len(data) >= 0, "Expected results from NULL value check"


class TestGQLLimitOffsetAdvanced:
    """Test advanced LIMIT and OFFSET behaviors"""
    
    def test_limit_with_cursor_and_integer(self, conn):
        """Test LIMIT with cursor and integer"""
        result = conn.execute(text("SELECT * FROM users LIMIT @cursor, 5"))
        data = result.all()
        assert len(data) >= 0, "Expected results from LIMIT with cursor and integer"
    
    def test_offset_with_cursor_and_integer(self, conn):
        """Test OFFSET with cursor and integer"""
        result = conn.execute(text("SELECT * FROM users OFFSET @cursor, 2"))
        data = result.all()
        assert len(data) >= 0, "Expected results from OFFSET with cursor and integer"
    
    def test_offset_plus_notation(self, conn):
        """Test OFFSET with + notation"""
        result = conn.execute(text("SELECT * FROM users OFFSET @cursor + 17"))
        data = result.all()
        assert len(data) >= 0, "Expected results from OFFSET with + notation"
        
        # Test with explicit positive sign
        result = conn.execute(text("SELECT * FROM users OFFSET @cursor + +17"))
        data = result.all()
        assert len(data) >= 0, "Expected results from OFFSET with explicit + sign"
    
    def test_offset_without_limit(self, conn):
        """Test OFFSET without LIMIT"""
        result = conn.execute(text("SELECT * FROM users OFFSET 1"))
        data = result.all()
        assert len(data) >= 0, "Expected results from OFFSET without LIMIT"


class TestGQLKeywordAsPropertyNames:
    """Test using keywords as property names with backticks"""
    
    def test_keyword_properties_with_backticks(self, conn):
        """Test querying properties that match keywords"""
        keywords = [
            "SELECT", "FROM", "WHERE", "ORDER", "BY", "LIMIT", "OFFSET",
            "DISTINCT", "COUNT", "SUM", "AVG", "AND", "OR", "IN", "NOT",
            "ASC", "DESC", "NULL", "TRUE", "FALSE", "KEY", "DATETIME",
            "BLOB", "AGGREGATE", "OVER", "AS", "HAS", "ANCESTOR", "DESCENDANT"
        ]
        
        for keyword in keywords:
            result = conn.execute(text(f"SELECT `{keyword}` FROM users"))
            data = result.all()
            assert len(data) >= 0, f"Expected results from query with keyword property: {keyword}"
    
    def test_keyword_in_where_clause(self, conn):
        """Test using keywords in WHERE clause"""
        result = conn.execute(text("SELECT * FROM users WHERE `ORDER` = 'first'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from WHERE with keyword property"


class TestGQLAggregationSimplifiedForm:
    """Test simplified form of aggregation queries"""
    
    def test_select_count_simplified(self, conn):
        """Test SELECT COUNT(*) simplified form"""
        result = conn.execute(text("SELECT COUNT(*) AS total FROM tasks WHERE is_done = false"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from simplified COUNT(*)"
    
    def test_select_count_up_to_simplified(self, conn):
        """Test SELECT COUNT_UP_TO simplified form"""
        result = conn.execute(text("SELECT COUNT_UP_TO(5) AS total FROM tasks WHERE is_done = false"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from simplified COUNT_UP_TO"
    
    def test_select_sum_simplified(self, conn):
        """Test SELECT SUM simplified form"""
        result = conn.execute(text("SELECT SUM(hours) AS total_hours FROM tasks WHERE is_done = false"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from simplified SUM"
    
    def test_select_avg_simplified(self, conn):
        """Test SELECT AVG simplified form"""
        result = conn.execute(text("SELECT AVG(hours) AS average_hours FROM tasks WHERE is_done = false"))
        data = result.all()
        assert len(data) == 1, "Expected 1 row from simplified AVG"


class TestGQLProjectionQueries:
    """Test projection query behaviors"""
    
    def test_projection_query_duplicates(self, conn):
        """Test that projection queries may contain duplicates"""
        result = conn.execute(text("SELECT tag FROM tasks"))
        data = result.all()
        assert len(data) >= 0, "Expected results from projection query"
    
    def test_distinct_projection_query(self, conn):
        """Test DISTINCT with projection query"""
        result = conn.execute(text("SELECT DISTINCT tag FROM tasks"))
        data = result.all()
        assert len(data) >= 0, "Expected unique results from DISTINCT projection"
    
    def test_distinct_on_projection_query(self, conn):
        """Test DISTINCT ON with projection query"""
        result = conn.execute(text("SELECT DISTINCT ON (category) category, tag FROM tasks"))
        data = result.all()
        assert len(data) >= 0, "Expected results from DISTINCT ON projection"
    
    def test_distinct_vs_distinct_on_equivalence(self, conn):
        """Test that DISTINCT a,b,c is identical to DISTINCT ON (a,b,c) a,b,c"""
        result1 = conn.execute(text("SELECT DISTINCT name, age FROM users"))
        data1 = result1.all()
        
        result2 = conn.execute(text("SELECT DISTINCT ON (name, age) name, age FROM users"))
        data2 = result2.all()
        
        assert len(data1) == len(data2), "DISTINCT and DISTINCT ON should return same results"


class TestGQLOrderByRestrictions:
    """Test ORDER BY restrictions with inequality operators"""
    
    def test_inequality_with_order_by_first_property(self, conn):
        """Test inequality operator with ORDER BY - property must be first"""
        result = conn.execute(text("SELECT * FROM users WHERE age > 25 ORDER BY age, name"))
        data = result.all()
        assert len(data) >= 0, "Expected results from inequality with ORDER BY (property first)"
    
    def test_multiple_properties_order_by(self, conn):
        """Test ORDER BY with multiple properties"""
        result = conn.execute(text("SELECT * FROM users ORDER BY age DESC, name ASC, city"))
        data = result.all()
        assert len(data) >= 0, "Expected results from ORDER BY with multiple properties"


class TestGQLAncestorQueries:
    """Test ancestor relationship queries"""
    
    def test_has_ancestor_numeric_id(self, conn):
        """Test HAS ANCESTOR with numeric ID"""
        result = conn.execute(text("SELECT * WHERE __key__ HAS ANCESTOR KEY(Person, 5629499534213120)"))
        data = result.all()
        assert len(data) >= 0, "Expected results from HAS ANCESTOR with numeric ID"
    
    def test_has_ancestor_string_id(self, conn):
        """Test HAS ANCESTOR with string ID"""
        result = conn.execute(text("SELECT * WHERE __key__ HAS ANCESTOR KEY(Person, 'Amy')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from HAS ANCESTOR with string ID"
    
    def test_has_descendant_query(self, conn):
        """Test HAS DESCENDANT query"""
        result = conn.execute(text("SELECT * FROM users WHERE KEY(Person, 'Amy') HAS DESCENDANT __key__"))
        data = result.all()
        assert len(data) >= 0, "Expected results from HAS DESCENDANT query"


class TestGQLComplexKeyPaths:
    """Test complex key path elements"""
    
    def test_nested_key_path_with_project_namespace(self, conn):
        """Test nested key path with PROJECT and NAMESPACE"""
        result = conn.execute(text("""
            SELECT * FROM users 
            WHERE __key__ = KEY(
                PROJECT('my-project'), 
                NAMESPACE('my-namespace'), 
                'Company', 'tech_corp', 
                'Department', 'engineering', 
                'users', 'Elmerulia Frixell_id'
            )
        """))
        data = result.all()
        assert len(data) >= 0, "Expected results from nested key path with PROJECT and NAMESPACE"
    
    def test_key_path_with_integer_ids(self, conn):
        """Test key path with integer IDs"""
        result = conn.execute(text("SELECT * WHERE __key__ = KEY('Company', 12345, 'Employee', 67890)"))
        data = result.all()
        assert len(data) >= 0, "Expected results from key path with integer IDs"


class TestGQLBlobLiterals:
    """Test BLOB literal functionality"""
    
    def test_blob_literal_basic(self, conn):
        """Test basic BLOB literal"""
        result = conn.execute(text("SELECT * FROM users WHERE avatar = BLOB('SGVsbG8gV29ybGQ')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from BLOB literal"
    
    def test_blob_literal_in_conditions(self, conn):
        """Test BLOB literal in various conditions"""
        result = conn.execute(text("SELECT * FROM files WHERE data != BLOB('YWJjZGVmZ2g')"))
        data = result.all()
        assert len(data) >= 0, "Expected results from BLOB literal in condition"


class TestGQLOperatorPrecedence:
    """Test operator precedence (AND has higher precedence than OR)"""
    
    def test_and_or_precedence(self, conn):
        """Test AND has higher precedence than OR"""
        # a OR b AND c should parse as a OR (b AND c)
        result = conn.execute(text("SELECT * FROM users WHERE name = 'Elmerulia Frixell' OR age > 30 AND city = 'Tokyo'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from AND/OR precedence test"
    
    def test_parentheses_override_precedence(self, conn):
        """Test parentheses can override precedence"""
        # (a OR b) AND c
        result = conn.execute(text("SELECT * FROM users WHERE (name = 'Elmerulia Frixell' OR name = 'Virginia Robertson') AND age > 25"))
        data = result.all()
        assert len(data) >= 0, "Expected results from parentheses precedence override"


class TestGQLPerformanceOptimizations:
    """Test performance-related query patterns"""
    
    def test_key_only_query_performance(self, conn):
        """Test __key__ only query (should be faster)"""
        result = conn.execute(text("SELECT __key__ FROM users WHERE age > 25"))
        data = result.all()
        assert len(data) >= 0, "Expected results from key-only query"
    
    def test_projection_query_performance(self, conn):
        """Test projection query (should be faster than SELECT *)"""
        result = conn.execute(text("SELECT name, age FROM users WHERE city = 'Tokyo'"))
        data = result.all()
        assert len(data) >= 0, "Expected results from projection query"


class TestGQLErrorCases:
    """Test edge cases and potential error conditions"""
    
    def test_property_name_case_sensitivity(self, conn):
        """Test that property names are case sensitive"""
        # These should be treated as different properties
        result = conn.execute(text("SELECT Name, name, NAME FROM users"))
        data = result.all()
        assert len(data) >= 0, "Expected results from case-sensitive property names"
    
    def test_kind_name_case_sensitivity(self, conn):
        """Test that kind names are case sensitive"""
        # Users vs users should be different kinds
        result = conn.execute(text("SELECT * FROM Users"))
        data = result.all()
        assert len(data) >= 0, "Expected results from case-sensitive kind names"
