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
import base64
import collections
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud.datastore.helpers import GeoPoint
from google.oauth2 import service_account
from requests import Response
from sqlalchemy import types
from sqlglot import exp, parse_one, tokenize, tokens
from sqlglot.tokens import TokenType

from . import _types

logger = logging.getLogger("sqlalchemy.dialects.datastore_dbapi")

apilevel = "2.0"
threadsafety = 2
paramstyle = "named"


# Required exceptions
class Warning(Exception):
    """Exception raised for important DB-API warnings."""


class Error(Exception):
    """Exception representing all non-warning DB-API errors."""


class InterfaceError(Error):
    """DB-API error related to the database interface."""


class DatabaseError(Error):
    """DB-API error related to the database."""


class DataError(DatabaseError):
    """DB-API error due to problems with the processed data."""


class OperationalError(DatabaseError):
    """DB-API error related to the database operation."""


class IntegrityError(DatabaseError):
    """DB-API error when integrity of the database is affected."""


class InternalError(DatabaseError):
    """DB-API error when the database encounters an internal error."""


class ProgrammingError(DatabaseError):
    """DB-API exception raised for programming errors."""


Column = collections.namedtuple(
    "Column",
    [
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    ],
)

type_map = {
    str: types.String,
    int: types.NUMERIC,
    float: types.FLOAT,
    bool: types.BOOLEAN,
    bytes: types.BINARY,
    datetime: types.DATETIME,
    datastore.Key: types.JSON,
    GeoPoint: types.JSON,
    list: types.JSON,
    dict: types.JSON,
    None.__class__: types.String,
}


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self._datastore_client = connection._client
        self.rowcount = -1
        self.arraysize = None
        self._query_data = None
        self._query_rows = None
        self._closed = False
        self.description = None
        self.lastrowid = None
        self.warnings: list[str] = []

    def execute(self, statements, parameters=None):
        """Execute a Datastore operation."""
        if self._closed:
            raise Error("Cursor is closed.")

        # Check for DML statements
        upper_statement = statements.upper().strip()
        if upper_statement.startswith("INSERT"):
            self._execute_insert(statements, parameters)
            return
        if upper_statement.startswith("UPDATE"):
            self._execute_update(statements, parameters)
            return
        if upper_statement.startswith("DELETE"):
            self._execute_delete(statements, parameters)
            return

        tokens = tokenize(statements)
        if self._is_derived_query(tokens):
            self.execute_orm(statements, parameters, tokens)
        else:
            self.gql_query(statements, parameters)

    def _execute_insert(self, statement: str, parameters=None):
        """Execute an INSERT statement using Datastore client."""
        if parameters is None:
            parameters = {}

        logging.debug(f"Executing INSERT: {statement} with parameters: {parameters}")

        try:
            # Parse INSERT statement using sqlglot
            parsed = parse_one(statement)
            if not isinstance(parsed, exp.Insert):
                raise ProgrammingError(f"Expected INSERT statement, got: {type(parsed)}")

            # Get table/kind name
            # For INSERT, parsed.this is a Schema containing the table and columns
            schema_expr = parsed.this
            if isinstance(schema_expr, exp.Schema):
                # Schema has 'this' which is the table
                table_expr = schema_expr.this
                if isinstance(table_expr, exp.Table):
                    kind = table_expr.name
                else:
                    kind = str(table_expr)
            elif isinstance(schema_expr, exp.Table):
                kind = schema_expr.name
            else:
                raise ProgrammingError("Could not determine table name from INSERT")

            # Get column names from Schema's expressions
            columns = []
            if isinstance(schema_expr, exp.Schema) and schema_expr.expressions:
                for col in schema_expr.expressions:
                    if hasattr(col, "name"):
                        columns.append(col.name)
                    else:
                        columns.append(str(col))

            # Get values
            values_list = []
            values_expr = parsed.args.get("expression")
            if values_expr and hasattr(values_expr, "expressions"):
                for tuple_expr in values_expr.expressions:
                    if hasattr(tuple_expr, "expressions"):
                        row_values = []
                        for val in tuple_expr.expressions:
                            row_values.append(self._parse_insert_value(val, parameters))
                        values_list.append(row_values)
            elif values_expr:
                # Single row VALUES clause
                row_values = []
                if hasattr(values_expr, "expressions"):
                    for val in values_expr.expressions:
                        row_values.append(self._parse_insert_value(val, parameters))
                    values_list.append(row_values)

            # Create entities and insert them
            entities_created = 0
            for row_values in values_list:
                # Create entity key (auto-generated)
                key = self._datastore_client.key(kind)
                entity = datastore.Entity(key=key)

                # Set entity properties
                for i, col in enumerate(columns):
                    if i < len(row_values):
                        entity[col] = row_values[i]

                # Put entity to datastore
                self._datastore_client.put(entity)
                entities_created += 1
                # Save the last inserted entity's key ID for lastrowid
                if entity.key.id is not None:
                    self.lastrowid = entity.key.id
                elif entity.key.name is not None:
                    # For named keys, use a hash of the name as a numeric ID
                    self.lastrowid = hash(entity.key.name) & 0x7FFFFFFFFFFFFFFF

            self.rowcount = entities_created
            self._query_rows = iter([])
            self.description = None

        except Exception as e:
            logging.error(f"INSERT failed: {e}")
            raise ProgrammingError(f"INSERT failed: {e}")

    def _execute_update(self, statement: str, parameters=None):
        """Execute an UPDATE statement using Datastore client."""
        if parameters is None:
            parameters = {}

        logging.debug(f"Executing UPDATE: {statement} with parameters: {parameters}")

        try:
            parsed = parse_one(statement)
            if not isinstance(parsed, exp.Update):
                raise ProgrammingError(f"Expected UPDATE statement, got: {type(parsed)}")

            # Get table/kind name
            table_expr = parsed.this
            if isinstance(table_expr, exp.Table):
                kind = table_expr.name
            else:
                raise ProgrammingError("Could not determine table name from UPDATE")

            # Get the WHERE clause to find the entity key
            where = parsed.args.get("where")
            if not where:
                raise ProgrammingError("UPDATE without WHERE clause is not supported")

            # Extract the key ID from WHERE clause (e.g., WHERE id = :id_1)
            entity_key_id = self._extract_key_id_from_where(where, parameters)
            if entity_key_id is None:
                raise ProgrammingError("Could not extract entity key from WHERE clause")

            # Get the entity
            key = self._datastore_client.key(kind, entity_key_id)
            entity = self._datastore_client.get(key)
            if entity is None:
                self.rowcount = 0
                self._query_rows = iter([])
                self.description = None
                return

            # Apply the SET values
            for set_expr in parsed.args.get("expressions", []):
                if isinstance(set_expr, exp.EQ):
                    col_name = set_expr.left.name if hasattr(set_expr.left, "name") else str(set_expr.left)
                    value = self._parse_update_value(set_expr.right, parameters)
                    entity[col_name] = value

            # Save the entity
            self._datastore_client.put(entity)
            self.rowcount = 1
            self._query_rows = iter([])
            self.description = None

        except Exception as e:
            logging.error(f"UPDATE failed: {e}")
            raise ProgrammingError(f"UPDATE failed: {e}") from e

    def _execute_delete(self, statement: str, parameters=None):
        """Execute a DELETE statement using Datastore client."""
        if parameters is None:
            parameters = {}

        logging.debug(f"Executing DELETE: {statement} with parameters: {parameters}")

        try:
            parsed = parse_one(statement)
            if not isinstance(parsed, exp.Delete):
                raise ProgrammingError(f"Expected DELETE statement, got: {type(parsed)}")

            # Get table/kind name
            table_expr = parsed.this
            if isinstance(table_expr, exp.Table):
                kind = table_expr.name
            else:
                raise ProgrammingError("Could not determine table name from DELETE")

            # Get the WHERE clause to find the entity key
            where = parsed.args.get("where")
            if not where:
                raise ProgrammingError("DELETE without WHERE clause is not supported")

            # Extract the key ID from WHERE clause
            entity_key_id = self._extract_key_id_from_where(where, parameters)
            if entity_key_id is None:
                raise ProgrammingError("Could not extract entity key from WHERE clause")

            # Delete the entity
            key = self._datastore_client.key(kind, entity_key_id)
            self._datastore_client.delete(key)
            self.rowcount = 1
            self._query_rows = iter([])
            self.description = None

        except Exception as e:
            logging.error(f"DELETE failed: {e}")
            raise ProgrammingError(f"DELETE failed: {e}") from e

    def _extract_key_id_from_where(self, where_expr, parameters: dict) -> Optional[int]:
        """Extract entity key ID from WHERE clause."""
        # Handle WHERE id = :param or WHERE id = value
        if isinstance(where_expr, exp.Where):
            where_expr = where_expr.this

        if isinstance(where_expr, exp.EQ):
            left = where_expr.left
            right = where_expr.right

            # Check if left side is 'id'
            col_name = left.name if hasattr(left, "name") else str(left)
            if col_name.lower() == "id":
                return self._parse_key_value(right, parameters)

        return None

    def _parse_key_value(self, val_expr, parameters: dict) -> Optional[int]:
        """Parse a value expression to get key ID."""
        if isinstance(val_expr, exp.Literal):
            if val_expr.is_number:
                return int(val_expr.this)
        elif isinstance(val_expr, exp.Placeholder):
            param_name = val_expr.name or val_expr.this
            if param_name in parameters:
                return int(parameters[param_name])
            if param_name.startswith(":"):
                param_name = param_name[1:]
                if param_name in parameters:
                    return int(parameters[param_name])
        elif isinstance(val_expr, exp.Parameter):
            param_name = val_expr.this.this if hasattr(val_expr.this, "this") else str(val_expr.this)
            if param_name in parameters:
                return int(parameters[param_name])
        return None

    def _parse_update_value(self, val_expr, parameters: dict) -> Any:
        """Parse a value expression from UPDATE SET clause."""
        if isinstance(val_expr, exp.Literal):
            if val_expr.is_string:
                return val_expr.this
            elif val_expr.is_number:
                text = val_expr.this
                if "." in text:
                    return float(text)
                return int(text)
            return val_expr.this
        elif isinstance(val_expr, exp.Null):
            return None
        elif isinstance(val_expr, exp.Boolean):
            return val_expr.this
        elif isinstance(val_expr, exp.Placeholder):
            param_name = val_expr.name or val_expr.this
            if param_name in parameters:
                return parameters[param_name]
            if param_name.startswith(":"):
                param_name = param_name[1:]
                if param_name in parameters:
                    return parameters[param_name]
            return None
        elif isinstance(val_expr, exp.Parameter):
            param_name = val_expr.this.this if hasattr(val_expr.this, "this") else str(val_expr.this)
            if param_name in parameters:
                return parameters[param_name]
            return None
        else:
            return str(val_expr.this) if hasattr(val_expr, "this") else str(val_expr)

    def _parse_insert_value(self, val_expr, parameters: dict) -> Any:
        """Parse a value expression from INSERT statement."""
        if isinstance(val_expr, exp.Literal):
            if val_expr.is_string:
                return val_expr.this
            elif val_expr.is_number:
                text = val_expr.this
                if "." in text:
                    return float(text)
                return int(text)
            return val_expr.this
        elif isinstance(val_expr, exp.Null):
            return None
        elif isinstance(val_expr, exp.Boolean):
            return val_expr.this
        elif isinstance(val_expr, exp.Placeholder):
            # Named parameter like :name
            param_name = val_expr.name or val_expr.this
            if param_name and param_name in parameters:
                return parameters[param_name]
            # Handle :name format
            if param_name and param_name.startswith(":"):
                param_name = param_name[1:]
                if param_name in parameters:
                    return parameters[param_name]
            return None
        elif isinstance(val_expr, exp.Parameter):
            # Named parameter
            param_name = val_expr.this.this if hasattr(val_expr.this, "this") else str(val_expr.this)
            if param_name in parameters:
                return parameters[param_name]
            return None
        else:
            # Try to get the string representation
            return str(val_expr.this) if hasattr(val_expr, "this") else str(val_expr)

    def _is_derived_query(self, tokens: List[tokens.Token]) -> bool:
        """
        Checks if the SQL statement contains a derived table (subquery in FROM).
        This is a more reliable way to distinguish complex ORM queries from simple GQL.
        """
        select_seen = 0
        for token in tokens:
            if token.token_type == TokenType.SELECT:
                select_seen += 1
                if select_seen >= 2:
                    return True
        return False

    def _is_aggregation_query(self, statement: str) -> bool:
        """Check if the statement contains aggregation functions."""
        upper = statement.upper()
        # Check for AGGREGATE ... OVER syntax
        if upper.strip().startswith("AGGREGATE"):
            return True
        # Check for aggregation functions in SELECT
        agg_patterns = [
            r"\bCOUNT\s*\(",
            r"\bCOUNT_UP_TO\s*\(",
            r"\bSUM\s*\(",
            r"\bAVG\s*\(",
        ]
        for pattern in agg_patterns:
            if re.search(pattern, upper):
                return True
        return False

    def _parse_aggregation_query(self, statement: str) -> Dict[str, Any]:
        """
        Parse aggregation query and return components.
        Returns dict with:
        - 'agg_functions': list of (func_name, column, alias)
        - 'base_query': the underlying SELECT query
        - 'is_aggregate_over': whether it's AGGREGATE...OVER syntax
        """
        upper = statement.upper().strip()
        result: Dict[str, Any] = {
            "agg_functions": [],
            "base_query": None,
            "is_aggregate_over": False,
        }

        # Handle AGGREGATE ... OVER (SELECT ...) syntax
        if upper.startswith("AGGREGATE"):
            result["is_aggregate_over"] = True
            # Extract the inner SELECT query
            over_match = re.search(
                r"OVER\s*\(\s*(SELECT\s+.+)\s*\)\s*$",
                statement,
                re.IGNORECASE | re.DOTALL,
            )
            if over_match:
                result["base_query"] = over_match.group(1).strip()
            else:
                # Fallback - extract everything after OVER
                over_idx = upper.find("OVER")
                if over_idx > 0:
                    # Extract content inside parentheses
                    remaining = statement[over_idx + 4 :].strip()
                    if remaining.startswith("("):
                        paren_depth = 0
                        for i, c in enumerate(remaining):
                            if c == "(":
                                paren_depth += 1
                            elif c == ")":
                                paren_depth -= 1
                                if paren_depth == 0:
                                    result["base_query"] = remaining[1:i].strip()
                                    break

            # Parse aggregation functions before OVER
            agg_part = statement[: upper.find("OVER")].strip()
            if agg_part.upper().startswith("AGGREGATE"):
                agg_part = agg_part[9:].strip()  # Remove "AGGREGATE"
            result["agg_functions"] = self._extract_agg_functions(agg_part)
        else:
            # Handle SELECT COUNT(*), SUM(col), etc.
            result["is_aggregate_over"] = False
            # Parse the SELECT clause to extract aggregation functions
            select_match = re.match(
                r"SELECT\s+(.+?)\s+FROM\s+(.+)$", statement, re.IGNORECASE | re.DOTALL
            )
            if select_match:
                select_clause = select_match.group(1)
                from_clause = select_match.group(2)
                result["agg_functions"] = self._extract_agg_functions(select_clause)
                # Build base query to get all data
                result["base_query"] = f"SELECT * FROM {from_clause}"
            else:
                # Handle SELECT without FROM (e.g., SELECT COUNT(*))
                select_match = re.match(
                    r"SELECT\s+(.+)$", statement, re.IGNORECASE | re.DOTALL
                )
                if select_match:
                    select_clause = select_match.group(1)
                    result["agg_functions"] = self._extract_agg_functions(select_clause)
                    result["base_query"] = None  # No base query for kindless

        return result

    def _extract_agg_functions(self, clause: str) -> List[Tuple[str, str, str]]:
        """Extract aggregation functions from a clause."""
        functions: List[Tuple[str, str, str]] = []
        # Pattern to match aggregation functions with optional alias
        patterns = [
            (
                r"COUNT_UP_TO\s*\(\s*(\d+)\s*\)(?:\s+AS\s+(\w+))?",
                "COUNT_UP_TO",
            ),
            (r"COUNT\s*\(\s*\*\s*\)(?:\s+AS\s+(\w+))?", "COUNT"),
            (r"SUM\s*\(\s*(\w+)\s*\)(?:\s+AS\s+(\w+))?", "SUM"),
            (r"AVG\s*\(\s*(\w+)\s*\)(?:\s+AS\s+(\w+))?", "AVG"),
        ]

        for pattern, func_name in patterns:
            for match in re.finditer(pattern, clause, re.IGNORECASE):
                if func_name == "COUNT":
                    col = "*"
                    alias = match.group(1) if match.group(1) else func_name
                elif func_name == "COUNT_UP_TO":
                    col = match.group(1)  # The limit number
                    alias = match.group(2) if match.group(2) else func_name
                else:
                    col = match.group(1)
                    alias = match.group(2) if match.group(2) else func_name
                functions.append((func_name, col, alias))

        return functions

    def _compute_aggregations(
        self,
        rows: List[Tuple],
        fields: Dict[str, Any],
        agg_functions: List[Tuple[str, str, str]],
    ) -> Tuple[List[Tuple], Dict[str, Any]]:
        """Compute aggregations on the data."""
        result_values: List[Any] = []
        result_fields: Dict[str, Any] = {}

        # Get column name to index mapping
        field_names = list(fields.keys())

        for func_name, col, alias in agg_functions:
            if func_name == "COUNT":
                value = len(rows)
            elif func_name == "COUNT_UP_TO":
                limit = int(col)
                value = min(len(rows), limit)
            elif func_name in ("SUM", "AVG"):
                # Find the column index
                if col in field_names:
                    col_idx = field_names.index(col)
                    values = [row[col_idx] for row in rows if row[col_idx] is not None]
                    numeric_values = [v for v in values if isinstance(v, (int, float))]
                    if func_name == "SUM":
                        value = sum(numeric_values) if numeric_values else 0
                    else:  # AVG
                        value = (
                            sum(numeric_values) / len(numeric_values)
                            if numeric_values
                            else 0
                        )
                else:
                    value = 0
            else:
                value = None

            result_values.append(value)
            result_fields[alias] = (alias, None, None, None, None, None, None)

        return [tuple(result_values)], result_fields

    def _execute_gql_request(self, gql_statement: str) -> Response:
        """Execute a GQL query and return the response."""
        body = {
            "gqlQuery": {
                "queryString": gql_statement,
                "allowLiterals": True,
            }
        }

        project_id = self._datastore_client.project
        if os.getenv("DATASTORE_EMULATOR_HOST") is None:
            credentials = getattr(
                self._datastore_client, "scoped_credentials", None
            )
            if credentials is None and self._datastore_client.credentials_info:
                credentials = service_account.Credentials.from_service_account_info(
                    self._datastore_client.credentials_info,
                    scopes=["https://www.googleapis.com/auth/datastore"],
                )
            if credentials is None:
                raise ProgrammingError(
                    "No credentials available for Datastore query. "
                    "Provide credentials_info, credentials_path, or "
                    "configure Application Default Credentials."
                )
            authed_session = AuthorizedSession(credentials)
            url = f"https://datastore.googleapis.com/v1/projects/{project_id}:runQuery"
            return authed_session.post(url, json=body)
        else:
            host = os.environ["DATASTORE_EMULATOR_HOST"]
            url = f"http://{host}/v1/projects/{project_id}:runQuery"
            return requests.post(url, json=body)

    def _needs_client_side_filter(self, statement: str) -> bool:
        """Check if the query needs client-side filtering due to unsupported ops.

        Note: This should be called on the CONVERTED GQL statement (after
        _convert_sql_to_gql), since that method handles reversing sqlglot
        transformations like <> -> != and NOT col IN -> col NOT IN.
        GQL natively supports: =, <, >, <=, >=, !=, IN, NOT IN, CONTAINS.
        """
        upper = statement.upper()
        unsupported_patterns = [
            r"\bOR\b",  # OR conditions need client-side evaluation
            r"\bBLOB\s*\(",  # BLOB literal (escaping issues)
        ]
        for pattern in unsupported_patterns:
            if re.search(pattern, upper):
                return True
        return False

    def _extract_base_query_for_filter(self, statement: str) -> str:
        """Extract base query without WHERE clause for client-side filtering."""
        # Remove WHERE clause to get all data
        upper = statement.upper()
        where_idx = upper.find(" WHERE ")
        if where_idx > 0:
            # Find the end of WHERE (before ORDER BY, LIMIT, OFFSET)
            end_patterns = [" ORDER BY ", " LIMIT ", " OFFSET "]
            end_idx = len(statement)
            for pattern in end_patterns:
                idx = upper.find(pattern, where_idx)
                if idx > 0 and idx < end_idx:
                    end_idx = idx
            # Remove WHERE clause
            base = statement[:where_idx] + statement[end_idx:]
            return base.strip()
        return statement

    def _is_missing_index_error(self, response: Response) -> bool:
        """Check if the GQL response indicates a missing composite index."""
        if response.status_code not in (400, 409):
            return False
        try:
            body = response.json()
            error = body.get("error", {})
            message = error.get("message", "").lower()
            status = error.get("status", "")
            return (
                "no matching index found" in message
                or status == "FAILED_PRECONDITION"
            )
        except Exception:
            return "no matching index" in response.text.lower()

    def _extract_table_only_query(self, gql_statement: str) -> str:
        """Extract just 'SELECT * FROM <table>' from a GQL statement."""
        table_match = re.search(
            r"\bFROM\s+(\w+)", gql_statement, flags=re.IGNORECASE
        )
        if table_match:
            return f"SELECT * FROM {table_match.group(1)}"
        raise ProgrammingError(
            f"Could not extract table name from query: {gql_statement}"
        )

    def _parse_order_by_clause(
        self, gql_statement: str
    ) -> List[Tuple[str, bool]]:
        """Parse ORDER BY clause. Returns list of (column, ascending) tuples."""
        upper = gql_statement.upper()
        order_idx = upper.find(" ORDER BY ")
        if order_idx < 0:
            return []
        # Find end of ORDER BY (before LIMIT, OFFSET)
        end_idx = len(gql_statement)
        for pattern in [" LIMIT ", " OFFSET "]:
            idx = upper.find(pattern, order_idx + 10)
            if 0 < idx < end_idx:
                end_idx = idx
        order_clause = gql_statement[order_idx + 10 : end_idx].strip()
        if not order_clause:
            return []
        result: List[Tuple[str, bool]] = []
        for part in order_clause.split(","):
            part = part.strip()
            parts = part.split()
            if not parts:
                continue
            col_name = parts[0]
            ascending = not (len(parts) > 1 and parts[1].upper() == "DESC")
            result.append((col_name, ascending))
        return result

    def _parse_limit_offset_clause(
        self, gql_statement: str
    ) -> Tuple[Optional[int], int]:
        """Parse LIMIT and OFFSET from statement. Returns (limit, offset)."""
        limit = None
        offset = 0
        limit_match = re.search(
            r"\bLIMIT\s+(\d+)", gql_statement, flags=re.IGNORECASE
        )
        if limit_match:
            limit = int(limit_match.group(1))
        offset_match = re.search(
            r"\bOFFSET\s+(\d+)", gql_statement, flags=re.IGNORECASE
        )
        if offset_match:
            offset = int(offset_match.group(1))
        return limit, offset

    def _apply_client_side_order_by(
        self,
        rows: List[Tuple],
        fields: Dict[str, Any],
        order_keys: List[Tuple[str, bool]],
    ) -> List[Tuple]:
        """Sort rows on the client side based on ORDER BY specification."""
        if not order_keys or not rows:
            return rows
        from functools import cmp_to_key

        field_names = list(fields.keys())

        def compare_rows(row_a: Tuple, row_b: Tuple) -> int:
            for col_name, ascending in order_keys:
                if col_name in field_names:
                    idx = field_names.index(col_name)
                    val_a = row_a[idx] if idx < len(row_a) else None
                    val_b = row_b[idx] if idx < len(row_b) else None
                else:
                    val_a = None
                    val_b = None
                if val_a is None and val_b is None:
                    continue
                if val_a is None:
                    return 1  # None sorts last
                if val_b is None:
                    return -1
                try:
                    if val_a < val_b:
                        cmp_result = -1
                    elif val_a > val_b:
                        cmp_result = 1
                    else:
                        continue
                except TypeError:
                    continue
                if not ascending:
                    cmp_result = -cmp_result
                return cmp_result
            return 0

        return sorted(rows, key=cmp_to_key(compare_rows))

    def _execute_fallback_query(
        self, original_statement: str, gql_statement: str
    ):
        """Execute a fallback query when the original fails due to missing index.

        Fetches all data from the table and applies WHERE, ORDER BY,
        LIMIT/OFFSET on the client side.
        """
        warning_msg = (
            "Missing index: the query requires an index that does not exist "
            "in Datastore. Falling back to fetching ALL entities and "
            "processing client-side (SELECT * mode). This may significantly "
            "increase query and egress costs. Consider adding the required "
            "composite index to avoid this."
        )
        logging.warning("%s Original GQL: %s", warning_msg, gql_statement)
        self.warnings.append(warning_msg)

        # Build simple query to fetch all data from the table
        fallback_query = self._extract_table_only_query(gql_statement)
        response = self._execute_gql_request(fallback_query)
        if response.status_code != 200:
            raise OperationalError(
                f"Fallback query failed: {fallback_query} "
                f"(original: {gql_statement})"
            )

        data = response.json()
        entity_results = data.get("batch", {}).get("entityResults", [])

        # Initialize cursor state for empty result
        self._query_data = iter([])
        self._query_rows = iter([])
        self.rowcount = 0
        self.description = [(None, None, None, None, None, None, None)]
        self._last_executed = original_statement
        self._parameters = {}

        if not entity_results:
            return

        self._closed = False

        # Parse entities with all columns (needed for filtering/sorting)
        rows, fields = ParseEntity.parse(entity_results, None)

        # Apply WHERE filter using the original statement to preserve
        # binary data in BLOB literals (whitespace normalization in
        # _convert_sql_to_gql would corrupt them).
        rows = self._apply_client_side_filter(rows, fields, original_statement)

        # Apply ORDER BY
        order_keys = self._parse_order_by_clause(gql_statement)
        if order_keys:
            rows = self._apply_client_side_order_by(rows, fields, order_keys)

        # Apply LIMIT/OFFSET
        limit, offset = self._parse_limit_offset_clause(gql_statement)
        if offset > 0:
            rows = rows[offset:]
        if limit is not None:
            rows = rows[:limit]

        # Project to requested columns if the original query specified them
        selected_columns = self._parse_select_columns(original_statement)
        if selected_columns is not None:
            field_names = list(fields.keys())
            projected_rows: List[Tuple] = []
            projected_fields: Dict[str, Any] = {}

            for col in selected_columns:
                col_lower = col.lower()
                if col_lower in ("__key__", "key") and "key" in fields:
                    projected_fields["key"] = fields["key"]
                elif col in fields:
                    projected_fields[col] = fields[col]

            for row in rows:
                new_row: List[Any] = []
                for col in selected_columns:
                    col_lower = col.lower()
                    lookup = "key" if col_lower in ("__key__", "key") else col
                    if lookup in field_names:
                        idx = field_names.index(lookup)
                        new_row.append(row[idx] if idx < len(row) else None)
                    else:
                        new_row.append(None)
                projected_rows.append(tuple(new_row))

            rows = projected_rows
            fields = projected_fields

        fields_list = list(fields.values())
        self._query_data = iter(rows)
        self._query_rows = iter(rows)
        self.rowcount = len(rows)
        self.description = fields_list if fields_list else None

    def _apply_client_side_filter(
        self, rows: List[Tuple], fields: Dict[str, Any], statement: str
    ) -> List[Tuple]:
        """Apply client-side filtering for unsupported WHERE conditions."""
        # Parse WHERE clause and apply filters
        upper = statement.upper()
        where_idx = upper.find(" WHERE ")
        if where_idx < 0:
            return rows

        # Find end of WHERE clause
        end_patterns = [" ORDER BY ", " LIMIT ", " OFFSET "]
        end_idx = len(statement)
        for pattern in end_patterns:
            idx = upper.find(pattern, where_idx)
            if idx > 0 and idx < end_idx:
                end_idx = idx

        where_clause = statement[where_idx + 7 : end_idx].strip()
        field_names = list(fields.keys())

        # Apply filter
        filtered_rows = []
        for row in rows:
            if self._evaluate_where(row, field_names, where_clause):
                filtered_rows.append(row)
        return filtered_rows

    def _evaluate_where(
        self, row: Tuple, field_names: List[str], where_clause: str
    ) -> bool:
        """Evaluate WHERE clause against a row. Returns True if row matches."""
        # Build a context dict from the row
        context = {}
        for i, name in enumerate(field_names):
            if i < len(row):
                context[name] = row[i]

        # Parse and evaluate the WHERE clause
        # This is a simplified evaluator for common patterns
        try:
            return self._eval_condition(context, where_clause)
        except Exception as e:
            logging.warning(
                "Client-side WHERE evaluation failed for clause '%s': %s. "
                "Row will be excluded (fail closed).",
                where_clause,
                e,
            )
            return False

    def _eval_condition(self, context: Dict[str, Any], condition: str) -> bool:
        """Evaluate a single condition or compound condition."""
        condition = condition.strip()

        # Handle parentheses
        if condition.startswith("(") and condition.endswith(")"):
            # Find matching paren
            depth = 0
            for i, c in enumerate(condition):
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        if i == len(condition) - 1:
                            return self._eval_condition(context, condition[1:-1])
                        break

        # Handle OR (lower precedence)
        or_match = re.search(r"\bOR\b", condition, re.IGNORECASE)
        if or_match:
            # Split on OR, but respect parentheses
            parts = self._split_on_operator(condition, "OR")
            if len(parts) > 1:
                return any(self._eval_condition(context, p) for p in parts)

        # Handle AND (higher precedence)
        and_match = re.search(r"\bAND\b", condition, re.IGNORECASE)
        if and_match:
            parts = self._split_on_operator(condition, "AND")
            if len(parts) > 1:
                return all(self._eval_condition(context, p) for p in parts)

        # Handle simple comparisons
        return self._eval_simple_condition(context, condition)

    def _split_on_operator(self, condition: str, operator: str) -> List[str]:
        """Split condition on operator while respecting parentheses."""
        parts: List[str] = []
        current = ""
        depth = 0
        i = 0
        pattern = re.compile(rf"\b{operator}\b", re.IGNORECASE)

        while i < len(condition):
            if condition[i] == "(":
                depth += 1
                current += condition[i]
            elif condition[i] == ")":
                depth -= 1
                current += condition[i]
            elif depth == 0:
                match = pattern.match(condition[i:])
                if match:
                    parts.append(current.strip())
                    current = ""
                    i += len(match.group()) - 1
                else:
                    current += condition[i]
            else:
                current += condition[i]
            i += 1

        if current.strip():
            parts.append(current.strip())
        return parts

    def _eval_simple_condition(self, context: Dict[str, Any], condition: str) -> bool:
        """Evaluate a simple comparison condition."""
        condition = condition.strip()

        # Handle __key__ = KEY(kind, value) comparison
        # Entity key is stored as "key" in context (from ParseEntity)
        key_eq_match = re.match(
            r"__key__\s*=\s*KEY\s*\(\s*\w+\s*,\s*(?:'([^']*)'|(\d+))\s*\)",
            condition,
            re.IGNORECASE,
        )
        if key_eq_match:
            key_name = key_eq_match.group(1)
            key_id = key_eq_match.group(2)
            field_val = context.get("key") or context.get("__key__")
            if isinstance(field_val, list) and len(field_val) > 0:
                last_path = field_val[-1]
                if isinstance(last_path, dict):
                    if key_name is not None:
                        return last_path.get("name") == key_name
                    if key_id is not None:
                        return str(last_path.get("id")) == key_id
            return False

        # Handle BLOB equality (before generic handlers, since BLOB literal
        # would confuse the generic _parse_literal path)
        blob_eq_match = re.match(
            r"(\w+)\s*=\s*BLOB\s*\('(.*?)'\)",
            condition,
            re.IGNORECASE | re.DOTALL,
        )
        if blob_eq_match:
            field = blob_eq_match.group(1)
            blob_str = blob_eq_match.group(2)
            try:
                blob_bytes = blob_str.encode("latin-1")
            except (UnicodeEncodeError, UnicodeDecodeError):
                blob_bytes = blob_str.encode("utf-8")
            field_val = context.get(field)
            if isinstance(field_val, bytes):
                return field_val == blob_bytes
            return False

        # Handle BLOB inequality
        blob_neq_match = re.match(
            r"(\w+)\s*!=\s*BLOB\s*\('(.*?)'\)",
            condition,
            re.IGNORECASE | re.DOTALL,
        )
        if blob_neq_match:
            field = blob_neq_match.group(1)
            blob_str = blob_neq_match.group(2)
            try:
                blob_bytes = blob_str.encode("latin-1")
            except (UnicodeEncodeError, UnicodeDecodeError):
                blob_bytes = blob_str.encode("utf-8")
            field_val = context.get(field)
            if isinstance(field_val, bytes):
                return field_val != blob_bytes
            return True

        # Handle NOT IN / NOT IN ARRAY
        not_in_match = re.match(
            r"(\w+)\s+NOT\s+IN\s+(?:ARRAY\s*)?\(([^)]+)\)",
            condition, re.IGNORECASE,
        )
        if not_in_match:
            field = not_in_match.group(1)
            values_str = not_in_match.group(2)
            values = self._parse_value_list(values_str)
            field_val = context.get(field)
            return field_val not in values

        # Handle IN / IN ARRAY
        in_match = re.match(
            r"(\w+)\s+IN\s+(?:ARRAY\s*)?\(([^)]+)\)",
            condition, re.IGNORECASE,
        )
        if in_match:
            field = in_match.group(1)
            values_str = in_match.group(2)
            values = self._parse_value_list(values_str)
            field_val = context.get(field)
            return field_val in values

        # Handle != and <>
        neq_match = re.match(r"(\w+)\s*(?:!=|<>)\s*(.+)", condition, re.IGNORECASE)
        if neq_match:
            field = neq_match.group(1)
            value = self._parse_literal(neq_match.group(2).strip())
            field_val = context.get(field)
            return field_val != value

        # Handle >=
        gte_match = re.match(r"(\w+)\s*>=\s*(.+)", condition)
        if gte_match:
            field = gte_match.group(1)
            value = self._parse_literal(gte_match.group(2).strip())
            field_val = context.get(field)
            if field_val is not None and value is not None:
                try:
                    return field_val >= value
                except TypeError:
                    return False
            return False

        # Handle <=
        lte_match = re.match(r"(\w+)\s*<=\s*(.+)", condition)
        if lte_match:
            field = lte_match.group(1)
            value = self._parse_literal(lte_match.group(2).strip())
            field_val = context.get(field)
            if field_val is not None and value is not None:
                try:
                    return field_val <= value
                except TypeError:
                    return False
            return False

        # Handle >
        gt_match = re.match(r"(\w+)\s*>\s*(.+)", condition)
        if gt_match:
            field = gt_match.group(1)
            value = self._parse_literal(gt_match.group(2).strip())
            field_val = context.get(field)
            if field_val is not None and value is not None:
                try:
                    return field_val > value
                except TypeError:
                    return False
            return False

        # Handle <
        lt_match = re.match(r"(\w+)\s*<\s*(.+)", condition)
        if lt_match:
            field = lt_match.group(1)
            value = self._parse_literal(lt_match.group(2).strip())
            field_val = context.get(field)
            if field_val is not None and value is not None:
                try:
                    return field_val < value
                except TypeError:
                    return False
            return False

        # Handle =
        eq_match = re.match(r"(\w+)\s*=\s*(.+)", condition)
        if eq_match:
            field = eq_match.group(1)
            value = self._parse_literal(eq_match.group(2).strip())
            field_val = context.get(field)
            return field_val == value

        # Default: include row
        return True

    def _parse_value_list(self, values_str: str) -> List[Any]:
        """Parse a comma-separated list of values."""
        values: List[Any] = []
        for v in values_str.split(","):
            values.append(self._parse_literal(v.strip()))
        return values

    def _parse_literal(self, literal: str) -> Any:
        """Parse a literal value from string."""
        literal = literal.strip()
        # DATETIME literal: DATETIME('2023-01-01T00:00:00Z')
        datetime_match = re.match(
            r"DATETIME\s*\(\s*'([^']*)'\s*\)", literal, re.IGNORECASE
        )
        if datetime_match:
            timestamp_str = datetime_match.group(1)
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str.replace("Z", "+00:00")
            # Normalize fractional seconds to 6 digits for Python 3.10
            # compatibility (fromisoformat only handles 0, 3, or 6 digits).
            frac_match = re.match(
                r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(.*)",
                timestamp_str,
            )
            if frac_match:
                frac = frac_match.group(2)[:6].ljust(6, "0")
                timestamp_str = (
                    frac_match.group(1) + "." + frac + frac_match.group(3)
                )
            return datetime.fromisoformat(timestamp_str)
        # String literal
        if (literal.startswith("'") and literal.endswith("'")) or (
            literal.startswith('"') and literal.endswith('"')
        ):
            return literal[1:-1]
        # Boolean
        if literal.upper() == "TRUE":
            return True
        if literal.upper() == "FALSE":
            return False
        # NULL
        if literal.upper() == "NULL":
            return None
        # Number
        try:
            if "." in literal:
                return float(literal)
            return int(literal)
        except ValueError:
            return literal

    def _is_orm_id_query(self, statement: str) -> bool:
        """Check if this is an ORM-style query with table.id in WHERE clause."""
        upper = statement.upper()
        # Check for patterns like "table.id = :param" in WHERE clause
        return (
            "SELECT" in upper
            and ".ID" in upper
            and "WHERE" in upper
            and (":PK_" in upper or ":ID_" in upper or ".ID =" in upper)
        )

    def _execute_orm_id_query(self, statement: str, parameters: dict):
        """Execute an ORM-style query by ID using direct key lookup."""
        try:
            parsed = parse_one(statement)
            if not isinstance(parsed, exp.Select):
                raise ProgrammingError("Expected SELECT statement")

            # Get table name
            from_arg = parsed.args.get("from") or parsed.args.get("from_")
            if not from_arg:
                raise ProgrammingError("Could not find FROM clause")
            table_name = from_arg.this.name if hasattr(from_arg.this, "name") else str(from_arg.this)

            # Extract column aliases from SELECT clause FIRST (before querying)
            # This ensures we have description even when no entity is found
            column_info = []
            for expr in parsed.expressions:
                if isinstance(expr, exp.Alias):
                    alias = expr.alias
                    if isinstance(expr.this, exp.Column):
                        col_name = expr.this.name
                    else:
                        col_name = str(expr.this)
                    column_info.append((col_name, alias))
                elif isinstance(expr, exp.Column):
                    col_name = expr.name
                    column_info.append((col_name, col_name))
                elif isinstance(expr, exp.Star):
                    # SELECT * - we'll handle this after fetching entity
                    column_info = None
                    break

            # Build description from column info (for non-SELECT * cases)
            if column_info is not None:
                field_names = [alias for _, alias in column_info]
                self.description = [
                    (name, None, None, None, None, None, None)
                    for name in field_names
                ]

            # Extract ID from WHERE clause
            where = parsed.args.get("where")
            if not where:
                raise ProgrammingError("Expected WHERE clause")

            entity_key_id = self._extract_key_id_from_where(where, parameters)
            if entity_key_id is None:
                raise ProgrammingError("Could not extract key ID from WHERE")

            # Fetch entity by key
            key = self._datastore_client.key(table_name, entity_key_id)
            entity = self._datastore_client.get(key)

            if entity is None:
                # No entity found - description is already set above
                self._query_rows = iter([])
                self.rowcount = 0
                # For SELECT *, set empty description since we don't know the schema
                if column_info is None:
                    self.description = []
                return

            # Build result row
            if column_info is None:
                # SELECT * case
                row_values = [entity.key.id]  # Add id first
                field_names = ["id"]
                for prop_name in sorted(entity.keys()):
                    row_values.append(entity[prop_name])
                    field_names.append(prop_name)
                # Build description for SELECT *
                self.description = [
                    (name, None, None, None, None, None, None)
                    for name in field_names
                ]
            else:
                row_values = []
                for col_name, alias in column_info:
                    if col_name.lower() == "id":
                        row_values.append(entity.key.id)
                    else:
                        row_values.append(entity.get(col_name))

            self._query_rows = iter([tuple(row_values)])
            self.rowcount = 1

        except Exception as e:
            logging.error(f"ORM ID query failed: {e}")
            raise ProgrammingError(f"ORM ID query failed: {e}") from e

    def _substitute_parameters(self, statement: str, parameters: dict) -> str:
        """Substitute named parameters in SQL statement with their values."""
        result = statement
        for param_name, value in parameters.items():
            # Build the placeholder pattern (e.g., :param_name)
            placeholder = f":{param_name}"

            # Format the value appropriately for GQL
            if value is None:
                formatted_value = "NULL"
            elif isinstance(value, str):
                # Escape single quotes in strings
                escaped = value.replace("'", "''")
                formatted_value = f"'{escaped}'"
            elif isinstance(value, bool):
                formatted_value = "true" if value else "false"
            elif isinstance(value, (int, float)):
                formatted_value = str(value)
            elif isinstance(value, datetime):
                # Format as ISO string for GQL
                formatted_value = f"DATETIME('{value.isoformat()}')"
            else:
                # Default to string representation
                formatted_value = f"'{str(value)}'"

            result = result.replace(placeholder, formatted_value)

        return result

    def gql_query(self, statement, parameters=None, **kwargs):
        """Execute a GQL query with support for aggregations."""

        # Check for ORM-style queries with table.id in WHERE clause
        if parameters and self._is_orm_id_query(statement):
            self._execute_orm_id_query(statement, parameters)
            return

        # Substitute parameters if provided
        if parameters:
            statement = self._substitute_parameters(statement, parameters)

        # Convert SQL to GQL-compatible format
        gql_statement = self._convert_sql_to_gql(statement)
        logging.debug(f"Converted GQL statement: {gql_statement}")

        # Check if this is an aggregation query
        if self._is_aggregation_query(statement):
            self._execute_aggregation_query(statement, parameters)
            return

        # Check if we need client-side filtering (check converted GQL)
        needs_filter = self._needs_client_side_filter(gql_statement)
        if needs_filter:
            # Get base query without unsupported WHERE conditions
            base_query = self._extract_base_query_for_filter(gql_statement)
            gql_statement = self._convert_sql_to_gql(base_query)

        # Execute GQL query
        response = self._execute_gql_request(gql_statement)

        if response.status_code == 200:
            data = response.json()
            logging.debug(data)
        else:
            # Fall back to client-side processing for any GQL failure.
            # The emulator may return 400 (INVALID_ARGUMENT for !=, NOT IN,
            # multi-value IN ARRAY), 409/400 (missing composite index), or
            # 500 (server error). In all cases we fetch all data from the
            # table and apply WHERE, ORDER BY, LIMIT/OFFSET client-side.
            # If even the simple fallback query fails, it raises an error.
            warning_msg = (
                "GQL query failed. Falling back to fetching ALL entities "
                "and processing client-side (SELECT * mode). This may "
                "significantly increase query and egress costs. Consider "
                "adding the required index for this query."
            )
            logging.warning(
                "%s (status %d)", warning_msg, response.status_code
            )
            self.warnings.append(warning_msg)
            self._execute_fallback_query(statement, statement)
            return

        self._query_data = iter([])
        self._query_rows = iter([])
        self.rowcount = 0
        self.description = [(None, None, None, None, None, None, None)]
        self._last_executed = statement
        self._parameters = parameters or {}

        data = data.get("batch", {}).get("entityResults", [])
        if len(data) == 0:
            return  # Everything is already set for an empty result

        # Determine if this statement is expected to return rows (e.g., SELECT)
        # You'll need a way to figure this out based on 'statement' or a flag passed to your custom execute method.
        # Example (simplified check, you might need a more robust parsing or flag):
        is_select_statement = statement.upper().strip().startswith("SELECT")

        if is_select_statement:
            self._closed = False  # For SELECT, cursor should remain open to fetch rows

            # Parse the SELECT statement to get column list
            selected_columns = self._parse_select_columns(statement)

            rows, fields = ParseEntity.parse(data, selected_columns)

            # Apply client-side filtering if needed.
            # Use the original statement (not the converted GQL) to preserve
            # binary data inside BLOB literals that whitespace normalization
            # in _convert_sql_to_gql would corrupt.
            if needs_filter:
                rows = self._apply_client_side_filter(rows, fields, statement)

            fields = list(fields.values())
            self._query_data = iter(rows)
            self._query_rows = iter(rows)
            self.rowcount = len(rows)
            self.description = fields if len(fields) > 0 else None
        else:
            # For INSERT/UPDATE/DELETE, the operation is complete, no rows to yield
            # For INSERT/UPDATE/DELETE, the operation is complete, set rowcount if possible
            affected_count = len(data) if isinstance(data, list) else 0
            self.rowcount = affected_count
            self._closed = True

    def _execute_aggregation_query(self, statement: str, parameters=None):
        """Execute an aggregation query with client-side aggregation."""
        parsed = self._parse_aggregation_query(statement)
        agg_functions = parsed["agg_functions"]
        base_query = parsed["base_query"]

        # If there's no base query and no functions, return empty
        if not agg_functions:
            self._query_rows = iter([])
            self.rowcount = 0
            self.description = []
            return

        # If there's no base query (e.g., SELECT COUNT(*) without FROM)
        # Return a count of 0 or handle specially
        if base_query is None:
            # For kindless COUNT(*), we return 0 since we can't query all kinds
            result_values: List[Any] = []
            result_fields: Dict[str, Any] = {}
            for func_name, col, alias in agg_functions:
                if func_name == "COUNT":
                    result_values.append(0)
                elif func_name == "COUNT_UP_TO":
                    result_values.append(0)
                else:
                    result_values.append(0)
                result_fields[alias] = (alias, None, None, None, None, None, None)

            self._query_rows = iter([tuple(result_values)])
            self.rowcount = 1
            self.description = list(result_fields.values())
            return

        # Convert to GQL first, then check for client-side filtering
        base_gql = self._convert_sql_to_gql(base_query)
        original_base_gql = base_gql  # Save for potential fallback
        needs_filter = self._needs_client_side_filter(base_gql)
        if needs_filter:
            filter_query = self._extract_base_query_for_filter(base_gql)
            base_gql = self._convert_sql_to_gql(filter_query)

        response = self._execute_gql_request(base_gql)

        if response.status_code != 200:
            warning_msg = (
                "Aggregation base query failed. Falling back to fetching "
                "ALL entities and aggregating client-side (SELECT * mode). "
                "This may significantly increase query and egress costs. "
                "Consider adding the required index for the columns used "
                "in this aggregation."
            )
            logging.warning(
                "%s (status %d)", warning_msg, response.status_code
            )
            self.warnings.append(warning_msg)
            fallback_query = self._extract_table_only_query(
                original_base_gql
            )
            response = self._execute_gql_request(fallback_query)
            if response.status_code != 200:
                raise OperationalError(
                    f"Aggregation fallback query failed: "
                    f"{fallback_query} (original: {statement})"
                )
            fb_data = response.json()
            fb_results = fb_data.get("batch", {}).get(
                "entityResults", []
            )
            if not fb_results:
                result_values: List[Any] = []
                result_fields: Dict[str, Any] = {}
                for _fn, _col, alias in agg_functions:
                    result_values.append(0)
                    result_fields[alias] = (
                        alias, None, None, None, None, None, None
                    )
                self._query_rows = iter([tuple(result_values)])
                self.rowcount = 1
                self.description = list(result_fields.values())
                return
            rows, fields = ParseEntity.parse(fb_results, None)
            rows = self._apply_client_side_filter(
                rows, fields, base_query
            )
            agg_rows, agg_fields = self._compute_aggregations(
                rows, fields, agg_functions
            )
            self._query_rows = iter(agg_rows)
            self.rowcount = len(agg_rows)
            self.description = list(agg_fields.values())
            return

        data = response.json()
        entity_results = data.get("batch", {}).get("entityResults", [])

        if len(entity_results) == 0:
            # No data - return aggregations with 0 values
            result_values = []
            result_fields: Dict[str, Any] = {}
            for func_name, _col, alias in agg_functions:
                if func_name == "COUNT":
                    result_values.append(0)
                elif func_name == "COUNT_UP_TO":
                    result_values.append(0)
                elif func_name in ("SUM", "AVG"):
                    result_values.append(0)
                else:
                    result_values.append(None)
                result_fields[alias] = (alias, None, None, None, None, None, None)

            self._query_rows = iter([tuple(result_values)])
            self.rowcount = 1
            self.description = list(result_fields.values())
            return

        # Parse the entity results
        rows, fields = ParseEntity.parse(entity_results, None)

        # Apply client-side filtering if needed
        if needs_filter:
            rows = self._apply_client_side_filter(rows, fields, base_query)

        # Compute aggregations
        agg_rows, agg_fields = self._compute_aggregations(rows, fields, agg_functions)

        self._query_rows = iter(agg_rows)
        self.rowcount = len(agg_rows)
        self.description = list(agg_fields.values())

    def execute_orm(
        self, statement: str, parameters=None, tokens: List[tokens.Token] = []
    ):
        if parameters is None:
            parameters = {}

        logging.debug(
            f"[DataStore DBAPI] Executing ORM query: {statement} with parameters: {parameters}"
        )

        statement = statement.replace("`", "'")
        parsed = parse_one(statement)
        # Note: sqlglot uses "from_" as the key, not "from"
        from_arg = parsed.args.get("from") or parsed.args.get("from_")
        if not isinstance(parsed, exp.Select) or not from_arg:
            raise ProgrammingError("Unsupported ORM query structure.")

        from_clause = from_arg.this
        if not isinstance(from_clause, exp.Subquery):
            raise ProgrammingError("Expected a subquery in the FROM clause.")

        subquery_sql = from_clause.this.sql()

        # 1. Query the subquery table
        self.gql_query(subquery_sql)
        subquery_results = self.fetchall()
        subquery_description = self.description

        # 2. Turn to pandas dataframe
        if not subquery_description:
            df = pd.DataFrame(subquery_results)
        else:
            column_names = [col[0] for col in subquery_description]
            df = pd.DataFrame(subquery_results, columns=column_names)

        # Add computed columns from SELECT expressions before grouping or ordering
        for p in parsed.expressions:
            if isinstance(p, exp.Alias) and not p.find(exp.AggFunc):
                # This is a simplified expression evaluator for computed columns.
                # It converts "col" to col and leaves other things as is.
                expr_str = re.sub(r'"(\w+)"', r"\1", p.this.sql())
                try:
                    # Use assign to add new columns based on expressions
                    df = df.assign(**{p.alias: df.eval(expr_str, engine="python")})
                except Exception as e:
                    logging.warning(f"Could not evaluate expression '{expr_str}': {e}")

        # 3. Apply outer query logic (aggregations and GROUP BY)
        has_agg = any(
            isinstance(p, exp.Alias) and p.find(exp.AggFunc)
            for p in parsed.expressions
        )

        if parsed.args.get("group"):
            group_by_cols = []
            for e in parsed.args.get("group").expressions:
                col_name = e.name if hasattr(e, "name") else ""
                if col_name and col_name in df.columns:
                    group_by_cols.append(col_name)
                else:
                    # Function expression (e.g. DATETIME_TRUNC) — find
                    # the matching alias in the SELECT clause.
                    expr_sql = e.sql()
                    matched = False
                    for p in parsed.expressions:
                        if isinstance(p, exp.Alias) and p.this.sql() == expr_sql:
                            group_by_cols.append(p.alias)
                            matched = True
                            break
                    if not matched:
                        group_by_cols.append(col_name)

            # Convert unhashable types (lists, dicts) to hashable types for groupby.
            # Datastore keys are stored as lists of dicts, GeoPoints as dicts.
            converted_cols = {}
            for col in group_by_cols:
                if col in df.columns:
                    sample = df[col].dropna().head(1)
                    if len(sample) > 0 and isinstance(sample.iloc[0], list):
                        converted_cols[col] = df[col].apply(
                            lambda x: tuple(
                                tuple(sorted(d.items()))
                                if isinstance(d, dict)
                                else d
                                for d in x
                            )
                            if isinstance(x, list)
                            else x
                        )
                        df[col] = converted_cols[col]
                    elif len(sample) > 0 and isinstance(sample.iloc[0], dict):
                        converted_cols[col] = df[col].apply(
                            lambda x: tuple(sorted(x.items()))
                            if isinstance(x, dict)
                            else x
                        )
                        df[col] = converted_cols[col]

            col_renames = {}
            for p in parsed.expressions:
                if isinstance(p, exp.Alias) and p.find(exp.AggFunc):
                    agg_func = p.this
                    agg_func_name = agg_func.key.lower()
                    # Map SQL aggregate names to pandas equivalents
                    sql_to_pandas_agg = {"avg": "mean"}
                    agg_func_name = sql_to_pandas_agg.get(
                        agg_func_name, agg_func_name
                    )
                    if agg_func.expressions:
                        original_col_name = agg_func.expressions[0].name
                    elif isinstance(agg_func.this, exp.Distinct):
                        # COUNT(DISTINCT col) - column is inside Distinct
                        original_col_name = (
                            agg_func.this.expressions[0].name
                        )
                        # Use pandas nunique for COUNT(DISTINCT)
                        agg_func_name = "nunique"
                    elif isinstance(agg_func.this, exp.Star):
                        # COUNT(*) - use first group_by column for counting
                        original_col_name = group_by_cols[0]
                    elif agg_func.this is not None and hasattr(
                        agg_func.this, "name"
                    ):
                        original_col_name = agg_func.this.name
                    else:
                        # Fallback for unknown structures
                        original_col_name = group_by_cols[0]
                    desired_sql_alias = p.alias_or_name
                    col_renames = {"temp_agg": desired_sql_alias}
                    df = (
                        df.groupby(group_by_cols)
                        .agg(temp_agg=(original_col_name, agg_func_name))
                        .reset_index()
                        .rename(columns=col_renames)
                    )

        elif has_agg:
            # Aggregation without GROUP BY (e.g., SELECT COUNT(*) FROM table)
            result_data: Dict[str, Any] = {}
            for p in parsed.expressions:
                if not isinstance(p, exp.Alias) or not p.find(exp.AggFunc):
                    continue
                agg_func = p.this
                agg_func_name = agg_func.key.lower()
                alias = p.alias_or_name

                if agg_func_name == "count":
                    if isinstance(agg_func.this, exp.Star):
                        result_data[alias] = len(df)
                    elif isinstance(agg_func.this, exp.Distinct):
                        col_name = agg_func.this.expressions[0].name
                        result_data[alias] = df[col_name].nunique()
                    elif agg_func.expressions:
                        col_name = agg_func.expressions[0].name
                        result_data[alias] = df[col_name].count()
                    else:
                        result_data[alias] = len(df)
                elif agg_func_name == "sum":
                    col_name = agg_func.this.name if agg_func.this else agg_func.expressions[0].name
                    result_data[alias] = df[col_name].sum()
                elif agg_func_name == "avg":
                    col_name = agg_func.this.name if agg_func.this else agg_func.expressions[0].name
                    result_data[alias] = df[col_name].mean()
                elif agg_func_name == "min":
                    col_name = agg_func.this.name if agg_func.this else agg_func.expressions[0].name
                    result_data[alias] = df[col_name].min()
                elif agg_func_name == "max":
                    col_name = agg_func.this.name if agg_func.this else agg_func.expressions[0].name
                    result_data[alias] = df[col_name].max()
                else:
                    result_data[alias] = None

            df = pd.DataFrame([result_data])

        if parsed.args.get("order"):
            order_by_cols = [e.this.name for e in parsed.args["order"].expressions]
            ascending = [
                not e.args.get("desc", False) for e in parsed.args["order"].expressions
            ]
            # Convert uncomparable types (dicts, lists) to strings for sorting.
            # Datastore keys are lists of dicts and GeoPoints are dicts, which
            # cannot be compared with < in Python 3.
            for col in order_by_cols:
                if col in df.columns:
                    sample = df[col].dropna().head(1)
                    if len(sample) > 0 and isinstance(
                        sample.iloc[0], (dict, list)
                    ):
                        df[col] = df[col].apply(
                            lambda x: str(x) if isinstance(x, (dict, list)) else x
                        )
            df = df.sort_values(by=order_by_cols, ascending=ascending)

        if parsed.args.get("limit"):
            limit = int(parsed.args["limit"].expression.sql())
            df = df.head(limit)

        # Final column selection
        if not any(isinstance(p, exp.Star) for p in parsed.expressions):
            final_columns = [p.alias_or_name for p in parsed.expressions]
            # Ensure all selected columns exist in the DataFrame before selecting
            df = df[[col for col in final_columns if col in df.columns]]

        # Finalize results
        rows = [tuple(x) for x in df.to_numpy()]
        schema = self._create_schema_from_df(df)
        self.rowcount = len(rows) if rows else 0
        self._set_description(schema)
        self._query_rows = iter(rows)

    def _create_schema_from_df(self, df: pd.DataFrame) -> tuple:
        """Create schema from a pandas DataFrame."""
        schema = []
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_string_dtype(dtype):
                sa_type = types.String
            elif pd.api.types.is_integer_dtype(dtype):
                sa_type = types.Integer
            elif pd.api.types.is_float_dtype(dtype):
                sa_type = types.Float
            elif pd.api.types.is_bool_dtype(dtype):
                sa_type = types.Boolean
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sa_type = types.DateTime
            else:
                sa_type = types.String  # Fallback

            schema.append(
                Column(
                    name=col_name,
                    type_code=sa_type(),
                    display_size=None,
                    internal_size=None,
                    precision=None,
                    scale=None,
                    null_ok=True,
                )
            )
        return tuple(schema)

    def _set_description(self, schema: tuple = ()):
        """Set the cursor description based on the schema."""
        self.description = schema

    def fetchall(self):
        if self._closed:
            raise Error("Cursor is closed.")
        return list(self._query_rows)

    def fetchmany(self, size=None):
        if self._closed:
            raise Error("Cursor is closed.")
        if size is None:
            size = self.arraysize or 1
        results = []
        for _ in range(size):
            try:
                results.append(next(self._query_rows))
            except StopIteration:
                break
        return results

    def fetchone(self):
        if self._closed:
            raise Error("Cursor is closed.")
        try:
            return next(self._query_rows)
        except StopIteration:
            return None

    def _parse_select_columns(self, statement: str) -> Optional[List[str]]:
        """
        Parse SELECT statement to extract column names.
        Returns None for SELECT * (all columns)
        """
        try:
            # Use sqlglot to parse the statement
            parsed = parse_one(statement)
            if not isinstance(parsed, exp.Select):
                return None

            columns = []
            for expr in parsed.expressions:
                if isinstance(expr, exp.Star):
                    # SELECT * - return None to indicate all columns
                    return None
                elif isinstance(expr, exp.Column):
                    # Direct column reference
                    col_name = expr.name
                    # Map 'id' to '__key__' since Datastore uses keys, not id properties
                    if col_name.lower() == "id":
                        col_name = "__key__"
                    columns.append(col_name)
                elif isinstance(expr, exp.Alias):
                    # Column with alias
                    if isinstance(expr.this, exp.Column):
                        col_name = expr.this.name
                        columns.append(col_name)
                    else:
                        # For complex expressions, use the alias
                        columns.append(expr.alias)
                else:
                    # For other expressions, try to get the name or use the string representation
                    col_name = expr.alias_or_name
                    if col_name:
                        columns.append(col_name)

            return columns if columns else None
        except Exception:
            # If parsing fails, return None to get all columns
            return None

    def _convert_sql_to_gql(self, statement: str) -> str:
        """
        Convert SQL statements to GQL-compatible format.

        GQL (Google Query Language) is similar to SQL but has its own syntax.
        This method reverses transformations applied by Superset's sqlglot
        processing (BigQuery dialect) and makes other adjustments for GQL
        compatibility.
        """
        # AGGREGATE queries are valid GQL - pass through directly
        if statement.strip().upper().startswith("AGGREGATE"):
            return statement

        # Normalize whitespace: sqlglot pretty-prints with newlines which
        # breaks position-based string operations (find, regex).
        statement = re.sub(r"\s+", " ", statement).strip()

        # === Reverse sqlglot / BigQuery dialect transformations ===

        # 1. Convert <> back to != (sqlglot BigQuery dialect converts != to <>)
        #    GQL uses != for not-equals comparisons.
        statement = re.sub(r"<>", "!=", statement)

        # 2. Fix NOT ... IN -> ... NOT IN
        #    sqlglot converts "col NOT IN (...)" to "NOT col IN (...)"
        #    GQL expects "col NOT IN (...)"
        statement = re.sub(
            r"\bNOT\s+(\w+)\s+IN\s*\(",
            r"\1 NOT IN (",
            statement,
            flags=re.IGNORECASE,
        )

        # 3. Strip ROW_NUMBER() OVER (...) added by sqlglot for DISTINCT ON
        #    BigQuery dialect converts "SELECT DISTINCT ON (col) * FROM t"
        #    to "SELECT *, ROW_NUMBER() OVER (PARTITION BY col ...) AS _row_... FROM t"
        #    We strip the ROW_NUMBER expression and any trailing WHERE _row_... = 1
        statement = re.sub(
            r",\s*ROW_NUMBER\s*\(\s*\)\s*OVER\s*\([^)]*\)\s*(?:AS\s+\w+)?",
            "",
            statement,
            flags=re.IGNORECASE,
        )
        # Also remove the WHERE _row_number = 1 subquery wrapper if present
        statement = re.sub(
            r"\bWHERE\s+_row_\w+\s*=\s*1\b",
            "",
            statement,
            flags=re.IGNORECASE,
        )

        # 4. Fix IN clause syntax for GQL
        #    a) Convert square bracket arrays: IN ['val'] -> IN ARRAY('val')
        #    b) Convert parenthesized lists: IN ('val1', 'val2') -> IN ARRAY('val1', 'val2')
        #    GQL requires the ARRAY keyword: "name IN ARRAY('val1', 'val2')"
        #    NOT IN also needs: "name NOT IN ARRAY('val1', 'val2')"
        statement = re.sub(
            r"\bIN\s*\[([^\]]*)\]",
            r"IN ARRAY(\1)",
            statement,
            flags=re.IGNORECASE,
        )
        # Convert IN (...) to IN ARRAY(...) but don't double-convert IN ARRAY(...)
        statement = re.sub(
            r"\bIN\s*\((?![\s]*SELECT\b)",
            "IN ARRAY(",
            statement,
            flags=re.IGNORECASE,
        )
        # Fix double ARRAY: if original was already ARRAY, we'd get IN ARRAY(ARRAY(...)
        statement = re.sub(
            r"\bARRAY\s*\(\s*ARRAY\s*\(",
            "ARRAY(",
            statement,
            flags=re.IGNORECASE,
        )

        # 5. Fix WHERE NULL (from sqlglot optimizing "col = NULL" to "NULL")
        #    sqlglot treats "col = NULL" as always-false and collapses to NULL.
        #    We can't recover the original column, but if the WHERE clause is
        #    just "WHERE NULL", remove it since it would return no results.
        statement = re.sub(
            r"\bWHERE\s+NULL\b",
            "",
            statement,
            flags=re.IGNORECASE,
        )

        # === GQL-specific transformations ===

        # Handle LIMIT FIRST(offset, count) syntax
        # Convert to LIMIT <count> OFFSET <offset>
        first_match = re.search(
            r"LIMIT\s+FIRST\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
            statement,
            flags=re.IGNORECASE,
        )
        if first_match:
            offset = first_match.group(1)
            count = first_match.group(2)
            statement = re.sub(
                r"LIMIT\s+FIRST\s*\(\s*\d+\s*,\s*\d+\s*\)",
                f"LIMIT {count} OFFSET {offset}",
                statement,
                flags=re.IGNORECASE,
            )

        # Extract table name from FROM clause for KEY() conversion
        table_match = re.search(
            r"\bFROM\s+(\w+)", statement, flags=re.IGNORECASE
        )
        table_name = table_match.group(1) if table_match else None

        # Remove DISTINCT ON (...) syntax - not supported by GQL.
        # GQL supports DISTINCT but not DISTINCT ON.
        statement = re.sub(
            r"\bDISTINCT\s+ON\s*\([^)]*\)\s*",
            "",
            statement,
            flags=re.IGNORECASE,
        )

        # Convert table.id in SELECT clause to __key__
        if table_name:
            statement = re.sub(
                rf"\b{table_name}\.id\b",
                "__key__",
                statement,
                flags=re.IGNORECASE,
            )

        # Handle bare 'id' references for GQL compatibility
        upper_stmt = statement.upper()
        from_pos = upper_stmt.find(" FROM ")
        if from_pos > 0:
            select_clause = statement[:from_pos]
            from_and_rest = statement[from_pos:]

            # Parse SELECT columns and remove id/__key__ from projection
            select_match = re.match(
                r"(SELECT\s+(?:DISTINCT\s+(?:ON\s*\([^)]*\)\s*)?)?)(.*)",
                select_clause,
                flags=re.IGNORECASE,
            )
            if select_match:
                prefix = select_match.group(1)
                cols_str = select_match.group(2)
                cols = [c.strip() for c in cols_str.split(",")]
                non_key_cols = [
                    c
                    for c in cols
                    if not re.match(
                        r"^(id|__key__)$", c.strip(), flags=re.IGNORECASE
                    )
                ]

                if not non_key_cols:
                    select_clause = prefix + "__key__"
                elif len(non_key_cols) < len(cols):
                    select_clause = prefix + ", ".join(non_key_cols)

            # Convert 'id' to '__key__' in WHERE/ORDER BY/etc.
            from_and_rest = re.sub(
                r"\bid\b", "__key__", from_and_rest, flags=re.IGNORECASE
            )

            statement = select_clause + from_and_rest
        else:
            statement = re.sub(
                r"\bid\b", "__key__", statement, flags=re.IGNORECASE
            )

        # Datastore restriction: projection queries with WHERE clauses require
        # composite indexes. Convert to SELECT * to avoid this requirement and
        # let ParseEntity handle column filtering from the full entity response.
        upper_check = statement.upper()
        from_check_pos = upper_check.find(" FROM ")
        where_check_pos = upper_check.find(" WHERE ")
        if from_check_pos > 0 and where_check_pos > from_check_pos:
            select_cols_str = re.sub(
                r"^SELECT\s+", "", statement[:from_check_pos], flags=re.IGNORECASE
            ).strip()
            if (
                select_cols_str != "*"
                and select_cols_str.upper() != "__KEY__"
                and not select_cols_str.upper().startswith("DISTINCT")
            ):
                statement = "SELECT * " + statement[from_check_pos + 1:]

        # Handle id = <number> in WHERE clauses -> KEY() syntax
        if table_name:
            id_where_match = re.search(
                r"\bWHERE\b.*\b(?:id|__key__)\s*=\s*(\d+)",
                statement,
                flags=re.IGNORECASE,
            )
            if id_where_match:
                id_value = id_where_match.group(1)
                statement = re.sub(
                    r"\b(?:id|__key__)\s*=\s*\d+",
                    f"__key__ = KEY({table_name}, {id_value})",
                    statement,
                    flags=re.IGNORECASE,
                )

        # Remove column aliases (AS alias_name) - GQL doesn't support them
        # But preserve AS inside AGGREGATE ... AS ... OVER syntax
        statement = re.sub(
            r"\bAS\s+\w+", "", statement, flags=re.IGNORECASE
        )

        # Remove table prefix from column names (table.column -> column)
        if table_name:
            statement = re.sub(
                rf"\b{table_name}\.(?!__)", "", statement, flags=re.IGNORECASE
            )

        # Clean up extra spaces and artifacts
        statement = re.sub(r"\s+", " ", statement).strip()
        statement = re.sub(r",\s*,", ",", statement)
        statement = re.sub(r"\s*,\s*\bFROM\b", " FROM", statement)

        return statement

    def close(self):
        self._closed = True
        self.connection = None
        logging.debug("Cursor is closed.")


class Connection:
    def __init__(self, client=None):
        self._client = client
        self._transaction = None

    def cursor(self):
        return Cursor(self)

    def begin(self):
        logging.debug("datastore connection transaction begin")

    def commit(self):
        logging.debug("datastore connection commit")

    def rollback(self):
        logging.debug("datastore connection rollback")

    def close(self):
        logging.debug("Closing connection")


def connect(client=None):
    return Connection(client)


class ParseEntity:
    @classmethod
    def parse(cls, data: dict, selected_columns: Optional[List[str]] = None):
        """
        Parse the datastore entity

        dict is a json base entity
        selected_columns: List of column names to include in results. If None, include all.
        """
        all_property_names_set = set()
        for entity_data in data:
            properties = entity_data.get("entity", {}).get("properties", {})
            all_property_names_set.update(properties.keys())

        # Determine which columns to include
        if selected_columns is None:
            # Include all properties if no specific selection
            sorted_property_names = sorted(list(all_property_names_set))
            include_key = True
        else:
            # Only include selected columns
            sorted_property_names = []
            include_key = False
            for col in selected_columns:
                if col.lower() == "__key__" or col.lower() == "key":
                    include_key = True
                elif col in all_property_names_set:
                    sorted_property_names.append(col)

        final_fields: dict = {}
        final_rows: List[Tuple] = []

        # Add key field if requested
        if include_key:
            final_fields["key"] = ("key", None, None, None, None, None, None)

        # Add selected fields in the order they appear in selected_columns if provided
        if selected_columns:
            # Keep the order from selected_columns
            for prop_name in selected_columns:
                if (
                    prop_name.lower() != "__key__"
                    and prop_name.lower() != "key"
                    and prop_name in all_property_names_set
                ):
                    final_fields[prop_name] = (
                        prop_name,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
        else:
            # Add all fields sorted by name
            for prop_name in sorted_property_names:
                final_fields[prop_name] = (
                    prop_name,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )

        # Append the properties
        for entity_data in data:
            row_values: List[Any] = []
            properties = entity_data.get("entity", {}).get("properties", {})
            key = entity_data.get("entity", {}).get("key", {})

            # Add key value if requested
            if include_key:
                row_values.append(key.get("path", []))

            # Append selected properties in the correct order
            if selected_columns:
                for prop_name in selected_columns:
                    if prop_name.lower() == "__key__" or prop_name.lower() == "key":
                        continue  # already added above
                    if prop_name in all_property_names_set:
                        prop_v = properties.get(prop_name)
                        if prop_v is not None:
                            prop_value, prop_type = ParseEntity.parse_properties(
                                prop_name, prop_v
                            )
                            row_values.append(prop_value)
                            current_field_info = final_fields[prop_name]
                            if (
                                current_field_info[1] is None
                                or current_field_info[1] == "UNKNOWN"
                            ):
                                final_fields[prop_name] = (
                                    prop_name,
                                    prop_type,
                                    current_field_info[2],
                                    current_field_info[3],
                                    current_field_info[4],
                                    current_field_info[5],
                                    current_field_info[6],
                                )
                        else:
                            row_values.append(None)
            else:
                # Append all properties in sorted order
                for prop_name in sorted_property_names:
                    prop_v = properties.get(prop_name)
                    if prop_v is not None:
                        prop_value, prop_type = ParseEntity.parse_properties(
                            prop_name, prop_v
                        )
                        row_values.append(prop_value)
                        current_field_info = final_fields[prop_name]
                        if (
                            current_field_info[1] is None
                            or current_field_info[1] == "UNKNOWN"
                        ):
                            final_fields[prop_name] = (
                                prop_name,
                                prop_type,
                                current_field_info[2],
                                current_field_info[3],
                                current_field_info[4],
                                current_field_info[5],
                                current_field_info[6],
                            )
                    else:
                        row_values.append(None)

            final_rows.append(tuple(row_values))

        return final_rows, final_fields

    @classmethod
    def parse_properties(cls, prop_k: str, prop_v: dict):
        value_type = next(iter(prop_v), None)
        prop_type = None
        prop_value: Any = None

        if value_type == "nullValue" or "nullValue" in prop_v:
            prop_value = None
            prop_type = _types.NULL_TYPE
        elif value_type == "booleanValue" or "booleanValue" in prop_v:
            prop_value = bool(prop_v["booleanValue"])
            prop_type = _types.BOOL
        elif value_type == "integerValue" or "integerValue" in prop_v:
            prop_value = int(prop_v["integerValue"])
            prop_type = _types.INTEGER
        elif value_type == "doubleValue" or "doubleValue" in prop_v:
            prop_value = float(prop_v["doubleValue"])
            prop_type = _types.FLOAT64
        elif value_type == "stringValue" or "stringValue" in prop_v:
            prop_value = prop_v["stringValue"]
            prop_type = _types.STRING
        elif value_type == "timestampValue" or "timestampValue" in prop_v:
            timestamp_str = prop_v["timestampValue"]
            if timestamp_str.endswith("Z"):
                # Handle ISO 8601 with Z suffix (UTC)
                prop_value = datetime.fromisoformat(
                    timestamp_str.replace("Z", "+00:00")
                )
            else:
                prop_value = datetime.fromisoformat(timestamp_str)
            prop_type = _types.TIMESTAMP
        elif value_type == "blobValue" or "blobValue" in prop_v:
            prop_value = base64.b64decode(prop_v.get("blobValue", b""))
            prop_type = _types.BYTES
        elif value_type == "geoPointValue" or "geoPointValue" in prop_v:
            prop_value = prop_v["geoPointValue"]
            prop_type = _types.GEOPOINT
        elif value_type == "keyValue" or "keyValue" in prop_v:
            prop_value = prop_v["keyValue"]["path"]
            prop_type = _types.KEY_TYPE
        elif value_type == "arrayValue" or "arrayValue" in prop_v:
            prop_value = []
            for entity in prop_v["arrayValue"].get("values", []):
                e_v, _ = ParseEntity.parse_properties(prop_k, entity)
                prop_value.append(e_v)
            prop_type = _types.ARRAY
        elif value_type == "dictValue" or "dictValue" in prop_v:
            prop_value = prop_v["dictValue"]
            prop_type = _types.STRUCT_FIELD_TYPES
        elif value_type == "entityValue" or "entityValue" in prop_v:
            prop_value = prop_v["entityValue"].get("properties") or {}
            prop_type = _types.STRUCT_FIELD_TYPES
        return prop_value, prop_type
