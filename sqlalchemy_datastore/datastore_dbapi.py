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

from google.cloud import datastore
from google.cloud.datastore.entity import Entity
from google.cloud.datastore import Key
from google.cloud.datastore.helpers import GeoPoint
from sqlalchemy import types
from datetime import datetime
from typing import Optional
import collections

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
    float: types.DOUBLE,    
    bool: types.BOOLEAN,     
    bytes: types.BINARY,   
    datetime: types.DATETIME, 
    datastore.Key: types.JSON,     
    GeoPoint: types.JSON, 
    list: types.JSON,       
    dict: types.JSON,      
    None.__class__: types.String
}

class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self._datastore_client = connection._client
        self.rowcount = -1
        self.arraysize = None 
        self._query_data = None
        self._query_rows = None
        self._result_set = None
        self._closed = False
        self.description = None

    def execute(self, operation: Optional[dict], parameters=None):
        if parameters is None:
            parameters = {}
            
        print(f"[DataStore DBAPI] Executing: {operation} with parameters: {parameters}")
        
        try:
            rows = self._execute(operation, **parameters)
            
            if isinstance(rows, list):
                # Case: query operation returns rows list
                rows = rows
                schema = self._infer_schema_from_rows(rows) if rows else ()
            elif isinstance(rows, dict):
                # Case: insert/update/delete operations return dict with status/id
                rows = [rows]
                schema = self._create_schema_from_dict(rows)
            else:
                # Fallback case
                rows = []
                schema = ()

            self.rowcount = len(rows) if rows else 0
            self._set_description(schema)
            self._query_rows = rows
            self._result_set = iter(rows)  # Set _result_set for fetch operations
            
        except Exception as e:
            self.rowcount = -1
            self._query_rows = []
            self._result_set = iter([])
            self.description = None
            raise OperationalError(f"Execution failed: {str(e)}", {}, None)

    def _execute(self, operation, **kw):
        """Execute the Datastore operation."""
        op_type = operation["type"]
        kind = operation["kind"]

        if op_type == "query":
            datastore_query = self._datastore_client.query(kind=kind)

            # Apply filters
            for prop, op, val in operation["filters"]:
                datastore_query.add_filter(prop, op, val)

            # Apply order by
            for prop, direction in operation["order_by"]:
                if direction == "ASCENDING":
                    datastore_query.order = [prop]
                else:
                    datastore_query.order = [f"-{prop}"]

            # Apply limit and offset
            limit = operation["limit"]
            offset = operation["offset"]

            results = []
            for entity in datastore_query.fetch(limit=limit, offset=offset):
                row_data = dict(entity)
                row_data["id"] = entity.key.id_or_name
                results.append(row_data)
            
            # Create schema from results
            schema = self._infer_schema_from_rows(results) if results else ()
            return results, schema

        elif op_type == "lookup":
            # Direct lookup by ID
            key_id = operation["id"]
            key = self._datastore_client.key(kind, key_id)
            entity = self._datastore_client.get(key)
            if entity:
                row_data = dict(entity)
                row_data["id"] = entity.key.id_or_name
                return [row_data]
            return []

        elif op_type == "insert":
            entity = datastore.Entity(
                self._datastore_client.key(kind, operation["key_name"])
                if operation["key_name"]
                else self._datastore_client.key(kind)
            )
            entity.update(operation["data"])
            self._datastore_client.put(entity)
            return {"id": entity.key.id_or_name}

        elif op_type == "update":
            key = self._datastore_client.key(kind, operation["id"])
            entity = self._datastore_client.get(key)
            if entity:
                entity.update(operation["data"])
                self._datastore_client.put(entity)
                return {"id": entity.key.id_or_name}
            raise OperationalError("Entity not found for update.", {}, None)

        elif op_type == "delete":
            key = self._datastore_client.key(kind, operation["id"])
            self._datastore_client.delete(key)
            return {"id": operation["id"]}

        elif op_type == "drop_kind":
            query = self._datastore_client.query(kind=kind)
            keys_to_delete = [entity.key for entity in query.fetch()]
            if keys_to_delete:
                self._datastore_client.delete_multi(keys_to_delete)
            return {"status": f"Deleted all entities of kind: {kind}"}

        elif op_type == "create_kind":
            return {"status": f"Kind {kind} acknowledged. No schema created."}

        else:
            raise OperationalError(f"Unsupported Datastore operation: {op_type}", {}, None)

    def _infer_schema_from_rows(self, rows):
        """Infer schema from the first row of data"""
        if not rows or not isinstance(rows[0], dict):
            return ()
        
        first_row = rows[0]
        schema = []
        for field_name, field_value in first_row.items():
            schema.append(
                Column(
                    name=field_name,
                    type_code=type_map.get(type(field_value), types.String)(),
                    display_size=None,
                    internal_size=None,
                    precision=None,
                    scale=None,
                    null_ok=True,
                )
            )
        return tuple(schema)

    def _create_schema_from_dict(self, result_dict):
        """Create schema from a result dictionary (for insert/update/delete operations)"""
        schema = []
        for field_name, field_value in result_dict.items():
            schema.append(
                Column(
                    name=field_name,
                    type_code=type_map.get(type(field_value), types.String)(),
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
        if self._result_set is None:
            return []
        return list(self._result_set)

    def fetchone(self):
        if self._closed:
            raise Error("Cursor is closed.")
        if self._result_set is None:
            return None
        try:
            return next(self._result_set)
        except StopIteration:
            return None

    def close(self):
        self._closed = True
        self.connection = None
        self._result_set = iter([])
        print("Cursor is closed.")

class Connection:
    def __init__(self, client=None):
        self._client = client
        self._transaction = None

    def cursor(self):
        return Cursor(self)

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

def connect(client=None):
    return Connection(client)