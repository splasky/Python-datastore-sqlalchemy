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
        self.rowcount = -1
        self.arraysize = 1
        self._query_data = None
        self._query_rows = None
        self._closed = False
        self.description = None


    def execute(self, operation ,parameters):
        print(f"[DataStore DBAPI] Executing: {operation} with parameters: {parameters}")

        self.description = []

    def _execute(self, is_delete=False):
        """
        No cursor here! We interact directly with the datastore client.
        """
        compiled = self.compiled.process(self.compiled.statement) # Get the output from your compiler

        operation = compiled.get('operation')
        
        if operation == 'insert':
            kind = compiled['kind']
            data = compiled['data']
            
            # Create a new entity
            key = self._datastore_client.key(kind)
            entity = datastore.Entity(key=key)
            entity.update(data)
            
            self._datastore_client.put(entity)
            print(f"Nya~! Inserted entity into kind '{kind}' with data: {data}")
            # Simulate a result set indicating success, or return the key
            self._result = [([{'inserted_id': key.id}],)] # Placeholder for Result object
        
        elif operation == 'select':
            params = compiled['params']
            query = self._datastore_client.query(kind=params['kind'])
            
            # Apply filters, order_by, etc. based on params
            # query.add_filter('property', '=', value)
            if 'projection' in params and params['projection']: # Select specific columns
                query.add_projection(params['projection'])

            results = list(query.fetch()) # Execute the query
            print(f"Nya~! Fetched {len(results)} entities from kind '{params['kind']}'")
            
            # Convert Datastore entities into a format SQLAlchemy Result can handle
            rows_for_sqla_result = []
            for entity in results:
                row_data = tuple(entity.get(col_name) for col_name in params['projection']) # Or all properties
                rows_for_sqla_result.append(row_data)
            
            # Store the results for SQLAlchemy to fetch
            # self._rowcount = len(rows_for_sqla_result) # Optional: if you need rowcount
            self._result_set = iter(rows_for_sqla_result) # Iterator of tuple rows
            
        else:
            raise NotImplementedError(
                f"Nya~! Datastore operation '{operation}' not yet implemented in execution context."
            )
        
        # This execute method in a real dialect should return `self._result_set`
        # for `Result` object to consume. Or it might leave it in a variable for `fetch_row`.

    def fetchall(self):
        return self.description

    def close(self):
        self.connection = None

    def fetchone(self):
        if self.description and len(self.description) > 0: 
            return self.description.pop(0)
        return None


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
