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
from typing import Optional

apilevel = "2.0"

# Threads may share the module and connections, but not cursors.
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
    """DB-API error related to the database operation.

    These errors are not necessarily under the control of the programmer.
    """


class IntegrityError(DatabaseError):
    """DB-API error when integrity of the database is affected."""


class InternalError(DatabaseError):
    """DB-API error when the database encounters an internal error."""


class ProgrammingError(DatabaseError):
    """DB-API exception raised for programming errors."""


class Cursor:

    def __init__(self, connection):
        self.connection = connection
        self._datastore_client = connection._client
        self.rowcount = -1
        self.arraysize = 1
        self._query_data = None
        self._query_rows = None
        self._closed = False
        self.description = None
        self.compiled: Optional[dict] = None  # This will hold the compiled statement
        self._closed = False

    def execute(self, operation: Optional[dict], parameters):
        print(f"[DataStore DBAPI] Executing: {operation} with parameters: {parameters}")
        self.compiled = operation 
        self._execute(operation, **parameters)
        self.description = []

    def _execute(self, operation, **kw):
        """
        This method is called when the compiler returns the compiled SQL.
        In our case, the compiler returns a dict representing the Datastore operation.
        """
        op_type = operation['type']
        kind = operation['kind']

        if op_type == 'query':
            datastore_query = self._datastore_client.query(kind=kind)

            # Apply filters
            for prop, op, val in operation['filters']:
                datastore_query.add_filter(prop, op, val)

            # Apply order by
            for prop, direction in operation['order_by']:
                if direction == 'ASCENDING':
                    datastore_query.order = [prop]
                else:
                    datastore_query.order = [f'-{prop}']

            # Apply limit and offset
            limit = operation['limit']
            offset = operation['offset']

            results = []
            for entity in datastore_query.fetch(limit=limit, offset=offset):
                # Convert Datastore Entity to a dictionary for SQLAlchemy result rows
                row_data = dict(entity)
                # Include the entity's ID (key.id_or_name)
                row_data['id'] = entity.key.id_or_name
                results.append(row_data)
            return results
        elif op_type == 'lookup':
            # Direct lookup by ID
            key_id = operation['id']
            key = self._datastore_client.key(kind, key_id)
            entity = self._datastore_client.get(key)
            if entity:
                row_data = dict(entity)
                row_data['id'] = entity.key.id_or_name
                return [row_data]
            return []
        elif op_type == 'insert':
            entity = datastore.Entity(self._datastore_client.key(kind, operation['key_name'])
                                      if operation['key_name'] else self._datastore_client.key(kind))
            entity.update(operation['data'])
            self._datastore_client.put(entity)
            # Return the key of the inserted entity, especially if ID was auto-generated
            return {'id': entity.key.id_or_name}
        elif op_type == 'update':
            key = self._datastore_client.key(kind, operation['id'])
            entity = self._datastore_client.get(key)
            if entity:
                entity.update(operation['data'])
                self._datastore_client.put(entity)
                return {'id': entity.key.id_or_name}
            raise OperationalError("Entity not found for update.", {}, None)
        elif op_type == 'delete':
            key = self._datastore_client.key(kind, operation['id'])
            self._datastore_client.delete(key)
            return {'id': operation['id']}
        elif op_type == 'drop_kind':
            # Warning: This is a highly destructive operation!
            # It will delete ALL entities of the specified kind.
            # In a real app, you'd likely have stricter controls or confirmation.
            query = self._datastore_client.query(kind=kind)
            keys_to_delete = [entity.key for entity in query.fetch()]
            if keys_to_delete:
                self._datastore_client.delete_multi(keys_to_delete)
            return {'status': f'Deleted all entities of kind: {kind}'}
        elif op_type == 'create_kind':
            # Datastore is schemaless, so 'create_table' is a no-op for actual schema.
            # It mainly confirms the 'kind' name can be used.
            return {'status': f'Kind {kind} acknowledged. No schema created.'}
        else:
            raise OperationalError(f"Unsupported Datastore operation: {op_type}", {}, None)
    
    def fetchall(self):
        if self._closed:
            raise Error("Cursor is closed.")
        return list(self._result_set)

    def fetchone(self):
        if self._closed:
            raise Error("Cursor is closed.")
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
