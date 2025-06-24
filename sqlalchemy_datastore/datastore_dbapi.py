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

apilevel = "2.0"

# Threads may share the module and connections, but not cursors.
threadsafety = 2

paramstyle = "pyformat"


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


import subprocess
import json
import pandas as pd

project_id = "test-api-2"
credentials_path = "test_credentials.json"

class Cursor:

    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1
        self.arraysize = 1
        self._query_data = None
        self._query_rows = None
        self._closed = False
        self.last_result = None

    def execute(self, sql, params=None):

        print(f"[DataStore DBAPI] Executing: {sql}")
        if self.connection._transaction is None:
            self.connection.begin()
        from importlib.resources import files

        jar_path = files("sqlalchemy_datastore").joinpath("gql-query.jar")
        print(jar_path)
        cmd = [
            "java",
            "-jar",
            jar_path,
            project_id,
            credentials_path,
            sql,
        ]
        # calling GQL
        res = subprocess.run(cmd, capture_output=True, text=True)
        df = pd.DataFrame(
            [json.loads(line) for line in res.stdout.strip().splitlines()]
        )
        print(df.head())

    def fetchall(self):
        return self.last_result

    def close(self):
        self.connection = None

    def fetchone(self):
        if self.last_result:
            return self.last_result.pop(0)
        return None


class Connection:

    def __init__(self, client=None):
        self._client = client
        self._transaction = None

    def cursor(self):
        return Cursor(self)

    def begin(self):
        if self._transaction is None:
            self._transaction = self._client.transaction()
            self._transaction.begin()

    def commit(self):
        if self._transaction:
            self._transaction.commit()
            self._transaction = None
        else:
            raise Exception("No active transaction")

    def rollback(self):
        # if self._transaction:
        #     self._transaction.rollback()
        #     self._transaction = None
        # else:
        #     raise Exception("No active transaction")
        pass


def connect(client=None):
    return Connection(client)
