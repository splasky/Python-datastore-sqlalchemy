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
import logging
from typing import Any, List, Optional
from concurrent import futures

from . import _types
from . import datastore_dbapi
from ._helpers import create_datastore_client
from .parse_url import parse_url

from sqlalchemy.engine import default, Connection
from sqlalchemy import exc
from sqlalchemy.sql import Select
from sqlalchemy.engine.interfaces import (
    DBAPICursor,
    _DBAPISingleExecuteParams,
    ExecutionContext,
)

from google.cloud import firestore_admin_v1
from google.cloud.firestore_admin_v1.types import Database
from google.oauth2 import service_account
import sqlparse

logger = logging.getLogger("sqlalchemy.dialects.CloudDatastore")


class CloudDatastoreDialect(default.DefaultDialect):
    """SQLAlchemy dialect for Google Cloud Datastore."""

    name = "datastore"
    driver = "datastore"

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
        self.credentials = None

    @classmethod
    def dbapi(cls):
        """Return the DBAPI 2.0 driver."""
        return datastore_dbapi

    def do_ping(self, dbapi_connection):
        """Performs a simple operation to check if the connection is still alive."""
        try:
            query = self._client.query(kind="__kind__")
            query.fetch(limit=1, timeout=30)
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

        self.credentials_path = credentials_path
        self.credentials_base64 = credentials_base64 or self.credentials_base64
        self.dataset_id = dataset_id
        self.billing_project_id = self.billing_project_id or self.project_id

        if user_supplied_client:
            return ([], {})
        else:
            client, credentials = create_datastore_client(
                credentials_path=self.credentials_path,
                credentials_info=self.credentials_info,
                credentials_base64=self.credentials_base64,
                project_id=self.billing_project_id,
                database=None,
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
        self.credentials = credentials
        setattr(self._client, "credentials_path", self.credentials_path)
        setattr(self._client, "credentials_info", self.credentials_info)
        setattr(self._client, "credentials_base64", self.credentials_base64)
        return ([], {"client": client})

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        return self._list_datastore_databases(self.credentials, self.project_id)

    def _list_datastore_databases(
        self, cred: service_account.Credentials, project_id: str
    ) -> List[str]:
        """Lists all Datastore databases for a given Google Cloud project."""
        client = firestore_admin_v1.FirestoreAdminClient(credentials=cred)
        parent = f"projects/{project_id}"

        try:
            list_database_resp = client.list_databases(parent=parent)

            def get_database_short_name(database: Database) -> Optional[List[str]]:
                full_name = database.name
                if full_name:
                    return full_name.split("/")[-1]
                return None

            with futures.ThreadPoolExecutor() as executor:
                schemas = list(
                    executor.map(get_database_short_name, list_database_resp.databases)
                )

            return schemas
        except Exception as e:
            logging.error(e)
        return []

    def get_table_names(
        self, connection: Connection, schema: str | None = None, **kw
    ) -> List[str]:
        client = self._client
        query = client.query(kind="__kind__")
        kinds = list(query.fetch())

        def get_kind_name(kind):
            return (
                name
                if (name := getattr(getattr(kind, "key", None), "name", None))
                is not None
                and isinstance(name, str)
                and not name.startswith("__")
                else None
            )

        with futures.ThreadPoolExecutor() as executor:
            result = list(executor.map(get_kind_name, kinds))

        return [t for t in result if t is not None]

    def get_columns(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ):
        """Retrieve column information from the database with optimized parallel processing."""
        client = self._client
        query = client.query(kind="__Stat_PropertyType_PropertyName_Kind__")
        query.add_filter("kind_name", "=", table_name)
        properties = list(query.fetch())

        def process_property(property):
            return {
                "name": property["property_name"],
                "type": _types._property_type[property["property_type"]],
                "nullable": True,
                "comment": "",
                "default": None,
            }

        with futures.ThreadPoolExecutor() as executor:
            columns = list(executor.map(process_property, properties))
        return columns

    def _contains_select_subquery(self, node) -> bool:
        """
        Check the AST node contains select subquery
        """
        if isinstance(node, Select):
            for child in node.get_children():
                if isinstance(child, Select) or self._contains_select_subquery(child):
                    return True

        for child in node.get_children():
            if isinstance(child, Select) or self._contains_select_subquery(child):
                return True

        return False

    def do_execute(
        self,
        cursor: DBAPICursor,
        statement: str,
        parameters: Optional[_DBAPISingleExecuteParams],
        context: Optional[ExecutionContext] = None,
    ) -> None:
        cursor.execute(statement)

    def get_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> List[str]:
        """
        Datastore doesn't have view, return empty list.
        """
        return []

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> bool:
        try:
            return table_name in self.get_table_names(connection, schema)
        except Exception as e:
            logging.debug(e)
            return False
