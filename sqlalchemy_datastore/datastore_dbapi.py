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
import base64
import logging
from google.cloud import datastore
from google.cloud.datastore.helpers import GeoPoint
from sqlalchemy import types
from datetime import datetime
from typing import Optional, Any, List, Tuple 
from . import _types
import requests
from requests import Response
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

import collections

logger = logging.getLogger('sqlalchemy.dialects.datastore_dbapi')

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

    def execute(self, statements, parameters=None):
        """Execute a Datastore operation."""
        if self._closed:
            raise Error("Cursor is closed.")
        self.gql_query(statements, parameters)

    def gql_query(self, statement, parameters=None, **kwargs):
        """Only execute raw SQL statements."""

        if os.getenv("DATASTORE_EMULATOR_HOST") is None:
            # Request service credentials
            credentials = service_account.Credentials.from_service_account_info(
                self._datastore_client.credentials_info,
                scopes=["https://www.googleapis.com/auth/datastore"],
            )

            # Create authorize session
            authed_session = AuthorizedSession(credentials)

        # GQL payload
        body = {
            "gqlQuery": {
                "queryString": statement,
                "allowLiterals": True,  # FIXME: This may cacuse sql injection
            }
        }

        response = Response()
        project_id = self._datastore_client.project
        url = f"https://datastore.googleapis.com/v1/projects/{project_id}:runQuery"
        if os.getenv("DATASTORE_EMULATOR_HOST") is None:
            response = authed_session.post(url, json=body)
        else:
            url = f"http://{os.environ["DATASTORE_EMULATOR_HOST"]}/v1/projects/{project_id}:runQuery"
            response = requests.post(url, json=body)

        if response.status_code == 200:
            data = response.json()
            logging.debug(data)
        else:
            logging.debug("Error:", response.status_code, response.text)
            raise OperationalError(
                f"Failed to execute statement:{statement}"
            )
        
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
            self._closed = (
                False  # For SELECT, cursor should remain open to fetch rows
            )

            rows, fields = ParseEntity.parse(data)
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

    def execute_orm(self, statements: Optional[dict], parameters=None):
        if parameters is None:
            parameters = {}

        logging.debug(f"[DataStore DBAPI] Executing: {statements} with parameters: {parameters}")

        try:
            result = self._execute(statements, **parameters)

            if isinstance(result, tuple) and len(result) == 2:
                # Case: query operation returns (rows, schema)
                rows, schema = result
            elif isinstance(result, list):
                # Fallback: treat as rows
                rows = result
                schema = self._infer_schema_from_rows(rows) if rows else ()
            elif isinstance(result, dict):
                # Case: insert/update/delete operations return dict with status/id
                rows = [result]
                schema = self._create_schema_from_dict(result)
            else:
                rows = []
                schema = ()

            self.rowcount = len(rows) if rows else 0
            self._set_description(schema)
            self._query_results = rows
            self._query_rows = iter(self._query_results)

        except Exception as e:
            self.rowcount = -1
            self._query_results = []
            self._query_rows = iter(self._query_results)
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
            raise OperationalError(
                f"Unsupported Datastore operation: {op_type}", {}, None
            )

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
        return list(self._query_rows)

    def fetchone(self):
        if self._closed:
            raise Error("Cursor is closed.")
        try:
            return next(self._query_rows)
        except StopIteration:
            return None

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
    def parse(cls, data: dict):
        """
        Parse the datastore entity

        dict is a json base entity
        """
        all_property_names_set = set()
        for entity_data in data:
            properties = entity_data.get("entity", {}).get("properties", {})
            all_property_names_set.update(properties.keys())

        # sort by names
        sorted_property_names = sorted(list(all_property_names_set))
        FieldDict = dict

        final_fields: FieldDict[str, Tuple] = FieldDict()
        final_rows: List[Tuple] = []

        # Add key fields, always the first fields
        final_fields["key"] = ("key", None, None, None, None, None, None) # None for type initially

        # Add other fields
        for prop_name in sorted_property_names:
            final_fields[prop_name] = (prop_name, None, None, None, None, None, None)

        # Append the properties
        for entity_data in data:
            row_values: List[Any] = []
            
            properties = entity_data.get("entity", {}).get("properties", {})
            key = entity_data.get("entity", {}).get("key", {})
            # add key fileds 
            row_values.append(key.get("path", []))

            # Append other properties according to the sorted properties
            for prop_name in sorted_property_names:
                prop_v = properties.get(prop_name)

                if prop_v is not None:
                    prop_value, prop_type = ParseEntity.parse_properties(prop_name, prop_v)
                    row_values.append(prop_value)
                    current_field_info = final_fields[prop_name]
                    if current_field_info[1] is None or current_field_info[1] == "UNKNOWN":
                        final_fields[prop_name] = (prop_name, prop_type, current_field_info[2], current_field_info[3], current_field_info[4], current_field_info[5], current_field_info[6])
                else:
                    row_values.append(None)
            
            final_rows.append(tuple(row_values))

        return final_rows, final_fields

    @classmethod
    def parse_properties(cls, prop_k: str, prop_v: dict):
        value_type = next(iter(prop_v), None)
        prop_type = None

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
            prop_value = datetime.fromisoformat(prop_v["timestampValue"])
            prop_type = _types.TIMESTAMP
        elif value_type == "blobValue" or "blobValue" in prop_v:
            prop_value = base64.b64decode(prop_v.get("blobValue", b''))
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
