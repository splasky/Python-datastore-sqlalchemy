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

from sqlalchemy.dialects import registry
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy import Engine
from urllib.parse import parse_qs

from . import datastore_dbapi
from ._helpers import create_datastore_client
from ._types import _get_sqla_column_type
from .parse_url import parse_url

registry.register("my_custom_dialect", "my_custom_dialect", "dialect")

from sqlalchemy.engine import default
from sqlalchemy import exc, types as sqltypes
from sqlalchemy.sql import compiler
from sqlalchemy.schema import CreateColumn, DropTable, CreateTable

# Import Google Cloud Datastore client library
from google.cloud import datastore

# Define constants for the dialect
class DatastoreCompiler(compiler.SQLCompiler):
    """
    Custom SQLCompiler for Google Cloud Datastore.
    Translates SQLAlchemy expressions into Datastore queries/operations.
    """
    def __init__(self, dialect, statement, *args, **kwargs):
        super(DatastoreCompiler, self).__init__(dialect, statement, *args, **kwargs)

    def visit_select(self, select_stmt, asfrom=False, **kw):
        """
        Handles SELECT statements.
        Datastore doesn't use SQL, so this translates to Datastore query objects.
        """
        # A very simplified approach. In a real dialect, this would
        # involve much more complex parsing of WHERE clauses, ORDER BY, LIMIT, etc.
        from_obj = select_stmt.froms[0]
        kind = from_obj.name # Assumes a single table and its name is the 'kind'

        if select_stmt._simple_int_clause is not None:
            # Handle primary key lookups if a specific ID is queried
            # This is a highly simplified example.
            pk_column = None
            for col in select_stmt.selected_columns:
                if col.primary_key:
                    pk_column = col
                    break
            if pk_column is not None:
                # Find the primary key value from the WHERE clause
                # This is a very naive way to get the ID for a direct lookup.
                # A proper implementation would parse the expression tree.
                pk_value = None
                if select_stmt._where_criteria:
                    # Look for comparison like 'id = value'
                    for criterion in select_stmt._where_criteria:
                        if hasattr(criterion, 'left') and hasattr(criterion, 'right'):
                            if hasattr(criterion.left, 'name') and criterion.left.name == pk_column.name:
                                pk_value = criterion.right.value
                                break
                if pk_value is not None:
                    # Return a special instruction for direct lookup by ID
                    # The execution context will handle this.
                    return {'kind': kind, 'id': pk_value, 'type': 'lookup'}


        # Build a basic query object for Datastore
        query = {
            'kind': kind,
            'filters': [], # Store filters here if WHERE clauses were parsed
            'order_by': [],
            'limit': select_stmt._limit,
            'offset': select_stmt._offset,
            'type': 'query'
        }

        # Simplified handling of WHERE clause (only direct comparisons for now)
        # A real dialect needs to traverse the expression tree.
        if select_stmt._where_criteria:
            for criterion in select_stmt._where_criteria:
                if hasattr(criterion, 'left') and hasattr(criterion, 'right') and hasattr(criterion, 'operator'):
                    col_name = criterion.left.name
                    op = criterion.operator.__name__ # e.g., 'eq', 'ne', 'gt', 'lt'
                    value = criterion.right.value

                    # Map SQLAlchemy operators to Datastore filter operators
                    datastore_op_map = {
                        'eq': '=',
                        'ne': '!=', # Not directly supported in Datastore, often requires two queries
                        'gt': '>',
                        'ge': '>=',
                        'lt': '<',
                        'le': '<='
                    }
                    if op in datastore_op_map:
                        query['filters'].append((col_name, datastore_op_map[op], value))
                    else:
                        # Handle other operators or raise error
                        pass

        # Handle order by
        for order in select_stmt._order_by_clause.clauses:
            column_name = order.element.name
            direction = 'ASCENDING' if order.is_ascending else 'DESCENDING'
            query['order_by'].append((column_name, direction))

        return query

    def visit_insert(self, insert_stmt, **kw):
        """
        Handles INSERT statements.
        """
        table = insert_stmt.table
        kind = table.name
        parameters = insert_stmt.parameters[0] # Assumes single parameter set for now

        # Datastore keys require a path. If primary key is provided, use it as ID.
        # Otherwise, Datastore generates one.
        key_name = None
        for col in table.columns:
            if col.primary_key and col.name in parameters:
                key_name = parameters[col.name]
                break

        return {'kind': kind, 'data': parameters, 'key_name': key_name, 'type': 'insert'}

    def visit_update(self, update_stmt, **kw):
        """
        Handles UPDATE statements.
        Requires a WHERE clause to identify the entity to update.
        """
        table = update_stmt.table
        kind = table.name
        parameters = update_stmt.parameters[0] # Assumes single parameter set for values to update

        # Extract primary key from WHERE clause
        key_name = None
        pk_column = None
        for col in table.columns:
            if col.primary_key:
                pk_column = col
                break

        if pk_column and update_stmt._where_criteria:
            # Simplified: assuming direct equality filter on PK for update
            for criterion in update_stmt._where_criteria:
                if hasattr(criterion, 'left') and hasattr(criterion, 'right'):
                    if hasattr(criterion.left, 'name') and criterion.left.name == pk_column.name:
                        key_name = criterion.right.value
                        break

        if key_name is None:
            raise exc.CompileError("UPDATE statement requires a primary key in WHERE clause for Datastore.")

        # Exclude the primary key from data to update if it's there
        data_to_update = {k: v for k, v in parameters.items() if k != pk_column.name}

        return {'kind': kind, 'id': key_name, 'data': data_to_update, 'type': 'update'}

    def visit_delete(self, delete_stmt, **kw):
        """
        Handles DELETE statements.
        Requires a WHERE clause to identify the entity to delete.
        """
        table = delete_stmt.table
        kind = table.name

        key_name = None
        pk_column = None
        for col in table.columns:
            if col.primary_key:
                pk_column = col
                break

        if pk_column and delete_stmt._where_criteria:
            # Simplified: assuming direct equality filter on PK for delete
            for criterion in delete_stmt._where_criteria:
                if hasattr(criterion, 'left') and hasattr(criterion, 'right'):
                    if hasattr(criterion.left, 'name') and criterion.left.name == pk_column.name:
                        key_name = criterion.right.value
                        break

        if key_name is None:
            raise exc.CompileError("DELETE statement requires a primary key in WHERE clause for Datastore.")

        return {'kind': kind, 'id': key_name, 'type': 'delete'}

    def visit_drop_table(self, drop, **kw):
        """
        Handles DROP TABLE statements.
        In Datastore, this means deleting all entities of a given 'kind'.
        This operation can be dangerous and expensive.
        """
        # This is a very destructive operation. Implement with caution.
        return {'kind': drop.element.name, 'type': 'drop_kind'}

    def visit_create_table(self, create, **kw):
        """
        Handles CREATE TABLE statements.
        Datastore is schemaless, so this primarily serves to acknowledge the
        'kind' name. It doesn't create a schema in the traditional sense.
        """
        # No actual schema creation in Datastore.
        # This just acknowledges the existence of a 'kind'.
        # A real dialect might use this to register expected properties for validation.
        return {'kind': create.element.name, 'type': 'create_kind'}


class DatastoreTypeCompiler(compiler.GenericTypeCompiler):
    """
    Type compiler for Datastore, mapping SQLAlchemy types to Datastore's implicit types.
    Datastore infers types, so this mostly handles basic scalar types.
    """
    def visit_INTEGER(self, type_, **kw):
        return None # Datastore infers numbers
    def visit_SMALLINT(self, type_, **kw):
        return None
    def visit_BIGINT(self, type_, **kw):
        return None
    def visit_BOOLEAN(self, type_, **kw):
        return None # Datastore infers booleans
    def visit_FLOAT(self, type_, **kw):
        return None # Datastore infers numbers
    def visit_NUMERIC(self, type_, **kw):
        return None
    def visit_DATETIME(self, type_, **kw):
        return None # Datastore infers datetime objects
    def visit_TIMESTAMP(self, type_, **kw):
        return None
    def visit_DATE(self, type_, **kw):
        return None # Datastore infers date objects 
    def visit_TIME(self, type_, **kw):
        return None
    def visit_VARCHAR(self, type_, **kw):
        return None # Datastore infers strings
    def visit_TEXT(self, type_, **kw):
        return None
    def visit_BLOB(self, type_, **kw):
        return None # Datastore handles bytes
    def visit_JSON(self, type_, **kw):
        return None # Datastore handles dicts/lists directly (as properties or embedded entities)


class CloudDatastoreDialect(default.DefaultDialect):
    """
    SQLAlchemy dialect for Google Cloud Datastore.
    """
    name = 'datastore'
    driver = 'google'

    # Specifies the compiler and execution context classes to use
    preparer = default.DefaultDialect.preparer
    statement_compiler = DatastoreCompiler
    type_compiler_cls = DatastoreTypeCompiler
    execution_ctx_cls = default.DefaultExecutionContext

    # Datastore does not have AUTOCOMMIT, explicit transactions are used
    # or mutations are atomic by default.
    supports_alter = False
    supports_pk_autoincrement = True # Datastore auto-generates IDs if not provided
    supports_sequences = False
    supports_comments = False
    supports_sane_rowcount = False # Not easily available in Datastore
    supports_schemas = False
    supports_foreign_keys = False
    supports_check_constraints = False
    supports_unique_constraint_initially_deferred = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None

    paramstyle = 'named' # Datastore client uses named parameters for queries

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

    # Required for connection. The URL format will be 'datastore:///?project_id=<your-project-id>'
    @classmethod
    def dbapi(cls):
        """
        Return the DBAPI 2.0 driver.
        In this case, it's the google.cloud.datastore client.
        """
        return datastore_dbapi

    def do_ping(self, dbapi_connection):
        """
        Performs a simple operation to check if the connection is still alive.
        """
        try:
            # Try to get a simple entity or list kinds to verify connectivity
            # This is a very basic ping. A more robust one might try a small query.
            dbapi_connection.get_default_project()
            return True
        except Exception:
            return False

    # def get_table_names(self, connection, schema=None, **kw):
    #     """
    #     Returns a list of 'kinds' (which are analogous to table names in Datastore).
    #     """
    #     kinds = set()
    #     # This is a bit tricky as Datastore doesn't have a direct 'list all kinds' API.
    #     # You often need to query the __Stat_Kind__ entities to find them.
    #     # This example uses a simplified approach that might not be comprehensive.
    #     # A more robust approach would query __Stat_Kind__ or iterate through metadata.
    #     # For this example, we'll return an empty list or rely on explicit table definitions.
    #     # If you know the kinds beforehand, you might pass them in config or load from a schema file.

    #     # Example of getting kinds from __Stat_Kind__ (requires appropriate indexes):
    #     client = connection.connection # This is our datastore.Client instance
    #     try:
    #         stat_query = client.query(kind='__Stat_Kind__')
    #         # Order by and distinct on 'kind_name' for better results, if supported by indexes
    #         # This is a simplified approach, direct 'fetch' is usually better for small stats
    #         for entity in stat_query.fetch():
    #             if 'kind_name' in entity:
    #                 kinds.add(entity['kind_name'])
    #     except Exception as e:
    #         # Handle cases where __Stat_Kind__ is not accessible or indexes are missing
    #         print(f"Warning: Could not retrieve kinds from __Stat_Kind__: {e}")
    #         pass

    #     return sorted(list(kinds))

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Datastore entities inherently have a primary key (the Key object),
        which is essentially the entity's ID.
        """
        # Assume 'id' is the primary key column name in the SQLAlchemy model
        return {'constrained_columns': ['id'], 'name': 'primary_key'}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Datastore does not support foreign keys in the traditional relational sense.
        Relationships are usually modeled via ancestor paths or by storing keys of related entities.
        """
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Datastore uses automatic and composite indexes.
        Retrieving them programmatically is complex via the API (usually done via gcloud commands or Console).
        This dialect will return an empty list.
        """
        return []

    def create_connect_args(self, url):
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
        """
        Parses the connection URL and returns args for the DBAPI connect function.
        URL format: datastore:///?project_id=<your_project_id>&namespace=<your_namespace>
        """

        self.arraysize = arraysize or self.arraysize
        self.list_tables_page_size = list_tables_page_size or self.list_tables_page_size
        self.location = location or self.location
        self.credentials_path = credentials_path or self.credentials_path
        self.credentials_base64 = credentials_base64 or self.credentials_base64
        self.dataset_id = dataset_id
        self.billing_project_id = self.billing_project_id or self.project_id

        if user_supplied_client:
            # The user is expected to supply a client with
            # create_engine('...', connect_args={'client': ds_client})
            return ([], {})
        else:
            client = create_datastore_client(
                credentials_path=self.credentials_path,
                credentials_info=self.credentials_info,
                credentials_base64=self.credentials_base64,
                project_id=self.billing_project_id,
            )
            # If the user specified `bigquery://` we need to set the project_id
            # from the client
            self.project_id = self.project_id or client.project
            self.billing_project_id = self.billing_project_id or client.project

        if not self.project_id:
            raise exc.ArgumentError("project_id is required for Datastore connection string.")
        self._client = client
        return ([], {"client": client})

    def get_table_names(self, connection, schema=None, **kw):
        # Implement logic to retrieve table names from the database
        if isinstance(connection, Engine):
            connection = connection.connect()

        client = connection.connection._client 
        query = client.query(kind="__kind__")
        query = query.keys_only()
        kinds = list(query.fetch())
        result = []
        for kind in kinds:
            result.append(kind.key.name)
        return result

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Datastore is schemaless, so columns are not explicitly defined.
        This method would typically inspect existing entities to infer properties.
        This is a complex operation and often not fully reliable without
        pre-defined schema information.
        For this basic dialect, we'll return an empty list.
        """
        # Implement logic to retrieve column information from the database
        if isinstance(connection, Engine):
            connection = connection.connect()
        client = connection.connection._client
        query = client.query(kind=table_name)
        ancestor_key = client.key("__kind__", "APIKey")
        query = client.query(kind="__property__", ancestor=ancestor_key)
        properties = list(query.fetch())
        columns = []
        # TODO: use _types.get_columns instead
        for property in properties:
            columns.append(
                {
                    "name": property.key.name,
                    "type": _get_sqla_column_type(property.get("property_representation")[0]
                                                  if property.get("property_representation", None) is not None else "STRING"),
                    "nullable": True,
                    "comment": "",
                    "default": None,
                }
            )

    def do_execute(self, cursor, statement, parameters, context=None):
        # Implement logic to execute a SQL statement
        cursor.execute(statement, parameters)
        return cursor.fetchall()
