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
import os

from . import _types
from . import datastore_dbapi
from ._helpers import create_datastore_client
from ._types import _get_sqla_column_type
from .parse_url import parse_url

from sqlalchemy.engine import default, Connection
from sqlalchemy import exc
from sqlalchemy.sql import compiler

# Define constants for the dialect
class DatastoreCompiler(compiler.SQLCompiler):
    """
    Custom SQLCompiler for Google Cloud Datastore.
    Translates SQLAlchemy expressions into Datastore queries/operations.
    """

    def __init__(self, dialect, statement, *args, **kwargs):
        super().__init__(dialect, statement, *args, **kwargs)

        if hasattr(statement, "_compile_state_factory"):
            compiler = dialect.statement_compiler(dialect, statement)
            self.compile_state = statement._compile_state_factory(statement, compiler)
            self.compiled.compile_state = self.compile_state
        else:
            self.compile_state = None

    def visit_select(self, select_stmt, asfrom=False, **kw):
        """
        Handles SELECT statements.
        Datastore doesn't use SQL, so this translates to Datastore query objects.
        """
        # A very simplified approach. In a real dialect, this would
        # involve much more complex parsing of WHERE clauses, ORDER BY, LIMIT, etc.

        # Get the table/kind name
        if hasattr(select_stmt, "table") and select_stmt.table is not None:
            kind = select_stmt.table.name
        elif hasattr(select_stmt, "froms") and select_stmt.froms:
            kind = select_stmt.froms[0].name
        else:
            raise exc.CompileError(
                "Cannot determine table/kind name from SELECT statement"
            )

        # Check for primary key lookup
        where_clause = getattr(select_stmt, "whereclause", None)
        if where_clause is not None:
            # Try to extract primary key value for direct lookup
            pk_value = self._extract_pk_value(where_clause, "id")
            if pk_value is not None:
                return {"kind": kind, "id": pk_value, "type": "lookup"}

        # Build a basic query object for Datastore
        query = {
            "kind": kind,
            "filters": [],
            "order_by": [],
            "limit": getattr(select_stmt, "_limit_clause", None),
            "offset": getattr(select_stmt, "_offset_clause", None),
            "type": "query",
        }

        # Parse WHERE clause
        if where_clause is not None:
            filters = self._parse_where_clause(where_clause)
            query["filters"] = filters

        # Parse ORDER BY clause
        order_by = getattr(select_stmt, "_order_by_clause", None)
        if order_by is not None:
            order_list = []
            for order in order_by.clauses:
                column_name = order.element.name
                direction = "ASCENDING" if order.is_ascending else "DESCENDING"
                order_list.append((column_name, direction))
            query["order_by"] = order_list

        return query

    def _extract_pk_value(self, where_clause, pk_column_name):
        """Extract primary key value from WHERE clause for direct lookup"""
        # This is a simplified implementation
        # In a real dialect, you'd need to traverse the expression tree more carefully
        if hasattr(where_clause, "left") and hasattr(where_clause, "right"):
            if (
                hasattr(where_clause.left, "name")
                and where_clause.left.name == pk_column_name
                and hasattr(where_clause, "operator")
                and where_clause.operator.__name__ == "eq"
            ):
                if hasattr(where_clause.right, "value"):
                    return where_clause.right.value
                else:
                    return where_clause.right
        return None

    def _parse_where_clause(self, where_clause):
        """Parse WHERE clause into Datastore filters"""
        filters = []

        if hasattr(where_clause, "left") and hasattr(where_clause, "right"):
            col_name = getattr(where_clause.left, "name", None)
            if col_name and hasattr(where_clause, "operator"):
                op = where_clause.operator.__name__
                value = getattr(where_clause.right, "value", where_clause.right)

                # Map SQLAlchemy operators to Datastore filter operators
                datastore_op_map = {
                    "eq": "=",
                    "ne": "!=",
                    "gt": ">",
                    "ge": ">=",
                    "lt": "<",
                    "le": "<=",
                }
                if op in datastore_op_map:
                    filters.append((col_name, datastore_op_map[op], value))

        return filters

    def visit_insert(self, insert_stmt, **kw):
        """Handles INSERT statements."""
        table = insert_stmt.table
        kind = table.name

        # Get parameters from the insert statement
        if hasattr(insert_stmt, "parameters") and insert_stmt.parameters:
            parameters = (
                insert_stmt.parameters[0]
                if isinstance(insert_stmt.parameters, list)
                else insert_stmt.parameters
            )
        else:
            parameters = {}

        # Datastore keys require a path. If primary key is provided, use it as ID.
        key_name = None
        for col in table.columns:
            if col.primary_key and col.name in parameters:
                key_name = parameters[col.name]
                break

        return {
            "kind": kind,
            "data": parameters,
            "key_name": key_name,
            "type": "insert",
        }

    def visit_update(self, update_stmt, **kw):
        """Handles UPDATE statements."""
        table = update_stmt.table
        kind = table.name

        # Get parameters
        if hasattr(update_stmt, "parameters") and update_stmt.parameters:
            parameters = (
                update_stmt.parameters[0]
                if isinstance(update_stmt.parameters, list)
                else update_stmt.parameters
            )
        else:
            parameters = {}

        # Extract primary key from WHERE clause
        key_name = None
        pk_column = None
        for col in table.columns:
            if col.primary_key:
                pk_column = col
                break

        if (
            pk_column
            and hasattr(update_stmt, "whereclause")
            and update_stmt.whereclause is not None
        ):
            key_name = self._extract_pk_value(update_stmt.whereclause, pk_column.name)

        if key_name is None:
            raise exc.CompileError(
                "UPDATE statement requires a primary key in WHERE clause for Datastore."
            )

        # Exclude the primary key from data to update if it's there
        data_to_update = {k: v for k, v in parameters.items() if k != pk_column.name}

        return {"kind": kind, "id": key_name, "data": data_to_update, "type": "update"}

    def visit_delete(self, delete_stmt, **kw):
        """Handles DELETE statements."""
        table = delete_stmt.table
        kind = table.name

        key_name = None
        pk_column = None
        for col in table.columns:
            if col.primary_key:
                pk_column = col
                break

        if (
            pk_column
            and hasattr(delete_stmt, "whereclause")
            and delete_stmt.whereclause is not None
        ):
            key_name = self._extract_pk_value(delete_stmt.whereclause, pk_column.name)

        if key_name is None:
            raise exc.CompileError(
                "DELETE statement requires a primary key in WHERE clause for Datastore."
            )

        return {"kind": kind, "id": key_name, "type": "delete"}

    def visit_drop_table(self, drop, **kw):
        """Handles DROP TABLE statements."""
        return {"kind": drop.element.name, "type": "drop_kind"}

    def visit_create_table(self, create, **kw):
        """Handles CREATE TABLE statements."""
        return {"kind": create.element.name, "type": "create_kind"}


class DatastoreTypeCompiler(compiler.GenericTypeCompiler):
    """Type compiler for Datastore, mapping SQLAlchemy types to Datastore's implicit types."""

    def visit_INTEGER(self, type_, **kw):
        return None

    def visit_SMALLINT(self, type_, **kw):
        return None

    def visit_BIGINT(self, type_, **kw):
        return None

    def visit_BOOLEAN(self, type_, **kw):
        return None

    def visit_FLOAT(self, type_, **kw):
        return None

    def visit_NUMERIC(self, type_, **kw):
        return None

    def visit_DATETIME(self, type_, **kw):
        return None

    def visit_TIMESTAMP(self, type_, **kw):
        return None

    def visit_DATE(self, type_, **kw):
        return None

    def visit_TIME(self, type_, **kw):
        return None

    def visit_VARCHAR(self, type_, **kw):
        return None

    def visit_TEXT(self, type_, **kw):
        return None

    def visit_BLOB(self, type_, **kw):
        return None

    def visit_JSON(self, type_, **kw):
        return None


class CloudDatastoreDialect(default.DefaultDialect):
    """SQLAlchemy dialect for Google Cloud Datastore."""

    name = "datastore"
    driver = "datastore"

    # Specify the compiler classes
    statement_compiler = DatastoreCompiler
    type_compiler_cls = DatastoreTypeCompiler

    # Datastore capabilities
    supports_alter = False
    supports_pk_autoincrement = True
    supports_sequences = False
    supports_comments = False
    supports_sane_rowcount = False
    supports_schemas = False
    supports_foreign_keys = False
    supports_check_constraints = False
    supports_unique_constraint_initially_deferred = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None

    paramstyle = "named"

    def __init__(
        self,
        arraysize=5000,
        credentials_path=None,
        billing_project_id=None,
        location=None,
        credentials_info=None,
        credentials_base64=None,
        list_tables_page_size=1000,
        *args,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.arraysize = arraysize
        self.credentials_path = credentials_path
        self.credentials_info = credentials_info
        self.credentials_base64 = credentials_base64
        self.project_id = None
        self.billing_project_id = billing_project_id
        self.location = location
        self.identifier_preparer = self.preparer(self)
        self.dataset_id = None
        self.list_tables_page_size = list_tables_page_size
        self._client = None

    @classmethod
    def dbapi(cls):
        """Return the DBAPI 2.0 driver."""
        return datastore_dbapi

    def do_ping(self, dbapi_connection):
        """Performs a simple operation to check if the connection is still alive."""
        try:
            # Basic connectivity check
            return True
        except Exception:
            return False

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Datastore entities inherently have a primary key (the Key object)."""
        return {"constrained_columns": ["id"], "name": "primary_key"}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Datastore does not support foreign keys."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Datastore uses automatic and composite indexes."""
        return []

    def create_connect_args(self, url):
        """Parses the connection URL and returns args for the DBAPI connect function."""
        (
            self.project_id,
            location,
            dataset_id,
            arraysize,
            credentials_path,
            credentials_base64,
            provided_job_config,
            list_tables_page_size,
            user_supplied_client,
        ) = parse_url(url)

        self.arraysize = arraysize or self.arraysize
        self.list_tables_page_size = list_tables_page_size or self.list_tables_page_size
        self.location = location or self.location
        # if (
        #     credentials_path is None
        #     and os.getenv("DATASTORE_EMULATOR_HOST") is None
        #     and os.getenv("GOOGLE_APPLICATION_CREDENTIALS") is None
        # ):
        #     raise ValueError(
        #         "credentials_path is required if GOOGLE_APPLICATION_CREDENTIALS is not set."
        #     )
        if os.getenv("DATASTORE_EMULATOR_HOST") is None:
            credentials_path = (
                credentials_path
                if credentials_path
                else os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            )
        self.credentials_path = credentials_path
        self.credentials_base64 = credentials_base64 or self.credentials_base64
        self.dataset_id = dataset_id
        self.billing_project_id = self.billing_project_id or self.project_id

        if user_supplied_client:
            return ([], {})
        else:
            client = create_datastore_client(
                credentials_path=self.credentials_path,
                credentials_info=self.credentials_info,
                credentials_base64=self.credentials_base64,
                project_id=self.billing_project_id,
            )
            self.project_id = self.project_id if self.project_id else client.project
            self.billing_project_id = (
                self.billing_project_id if self.billing_project_id else client.project
            )

        if not self.project_id:
            raise exc.ArgumentError(
                "project_id is required for Datastore connection string."
            )

        self._client = client
        setattr(self._client, "credentials_path", self.credentials_path)
        setattr(self._client, "credentials_info", self.credentials_info)
        setattr(self._client, "credentials_base64", self.credentials_base64)
        return ([], {"client": client})

    def get_schema_names(self, connection, **kw):
        return self.get_table_names(connection, None)

    def get_table_names(self, connection, schema: str | None = None, **kw):
        client = self._client
        query = client.query(kind="__kind__")
        kinds = list(query.fetch())
        result = []
        for kind in kinds:
            result.append(kind.key.name)
        return result

    def get_columns(connection: Connection, table_name: str, schema: str | None = None, **kw):
        """Retrieve column information from the database."""
        client = self._client
        query = client.query(kind=table_name)
        ancestor_key = client.key("__kind__", table_name)
        query = client.query(kind="__property__", ancestor=ancestor_key)
        properties = list(query.fetch())
        columns = []

        for property in properties:
            columns.append(
                {
                    "name": property.key.name,
                    "type": _types.STRING, # FIXME: Prototype usage, change later
                    "nullable": True,
                    "comment": "",
                    "default": None,
                }
            )
        return columns

    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a statement."""
        cursor.execute(statement, parameters)
