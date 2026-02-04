"""Unit tests for base.py CloudDatastoreDialect (no emulator required)."""
from unittest.mock import MagicMock

from sqlalchemy_datastore import CloudDatastoreDialect, datastore_dbapi

# ---------------------------------------------------------------------------
# Dialect metadata
# ---------------------------------------------------------------------------

def test_dialect_name():
    d = CloudDatastoreDialect()
    assert d.name == "datastore"
    assert d.driver == "datastore"


def test_dialect_capabilities():
    d = CloudDatastoreDialect()
    assert d.supports_alter is False
    assert d.supports_pk_autoincrement is True
    assert d.supports_sequences is False
    assert d.supports_comments is False
    assert d.supports_sane_rowcount is False
    assert d.supports_schemas is False
    assert d.supports_foreign_keys is False
    assert d.supports_check_constraints is False
    assert d.supports_unicode_statements is True
    assert d.supports_unicode_binds is True
    assert d.returns_unicode_strings is True
    assert d.paramstyle == "named"


def test_dialect_init_defaults():
    d = CloudDatastoreDialect()
    assert d.arraysize == 5000
    assert d.credentials_path is None
    assert d.credentials_info is None
    assert d.credentials_base64 is None
    assert d.billing_project_id is None
    assert d.location is None
    assert d.list_tables_page_size == 1000
    assert d._client is None


def test_dialect_init_custom():
    d = CloudDatastoreDialect(
        arraysize=100,
        credentials_path="/path/to/creds.json",
        billing_project_id="my-project",
        location="us-east1",
        list_tables_page_size=500,
    )
    assert d.arraysize == 100
    assert d.credentials_path == "/path/to/creds.json"
    assert d.billing_project_id == "my-project"
    assert d.location == "us-east1"
    assert d.list_tables_page_size == 500


# ---------------------------------------------------------------------------
# dbapi()
# ---------------------------------------------------------------------------

def test_dbapi_returns_module():
    result = CloudDatastoreDialect.dbapi()
    assert result is datastore_dbapi


# ---------------------------------------------------------------------------
# get_pk_constraint
# ---------------------------------------------------------------------------

def test_get_pk_constraint():
    d = CloudDatastoreDialect()
    result = d.get_pk_constraint(None, "users")
    assert result == {"constrained_columns": ["id"], "name": "primary_key"}


# ---------------------------------------------------------------------------
# get_foreign_keys
# ---------------------------------------------------------------------------

def test_get_foreign_keys():
    d = CloudDatastoreDialect()
    result = d.get_foreign_keys(None, "users")
    assert result == []


# ---------------------------------------------------------------------------
# get_indexes
# ---------------------------------------------------------------------------

def test_get_indexes():
    d = CloudDatastoreDialect()
    result = d.get_indexes(None, "users")
    assert result == []


# ---------------------------------------------------------------------------
# get_view_names
# ---------------------------------------------------------------------------

def test_get_view_names():
    d = CloudDatastoreDialect()
    result = d.get_view_names(None)
    assert result == []


# ---------------------------------------------------------------------------
# do_execute
# ---------------------------------------------------------------------------

def test_do_execute():
    d = CloudDatastoreDialect()
    cursor = MagicMock()
    d.do_execute(cursor, "SELECT * FROM users", {})
    cursor.execute.assert_called_once_with("SELECT * FROM users", {})


# ---------------------------------------------------------------------------
# do_ping
# ---------------------------------------------------------------------------

def test_do_ping_success():
    d = CloudDatastoreDialect()
    d._client = MagicMock()
    query_mock = MagicMock()
    d._client.query.return_value = query_mock
    result = d.do_ping(None)
    assert result is True
    d._client.query.assert_called_once_with(kind="__kind__")


def test_do_ping_failure():
    d = CloudDatastoreDialect()
    d._client = MagicMock()
    d._client.query.side_effect = Exception("connection failed")
    result = d.do_ping(None)
    assert result is False


# ---------------------------------------------------------------------------
# has_table
# ---------------------------------------------------------------------------

def test_has_table_true():
    d = CloudDatastoreDialect()
    d._client = MagicMock()

    # Mock get_table_names to return a list containing the table
    kind_entity = MagicMock()
    kind_entity.key.name = "users"
    d._client.query.return_value.fetch.return_value = [kind_entity]

    result = d.has_table(None, "users")
    assert result is True


def test_has_table_false():
    d = CloudDatastoreDialect()
    d._client = MagicMock()

    kind_entity = MagicMock()
    kind_entity.key.name = "users"
    d._client.query.return_value.fetch.return_value = [kind_entity]

    result = d.has_table(None, "nonexistent")
    assert result is False


def test_has_table_exception():
    d = CloudDatastoreDialect()
    d._client = MagicMock()
    d._client.query.side_effect = Exception("fail")
    result = d.has_table(None, "users")
    assert result is False


# ---------------------------------------------------------------------------
# get_table_names
# ---------------------------------------------------------------------------

def test_get_table_names():
    d = CloudDatastoreDialect()
    d._client = MagicMock()

    # Create mock entities with key.name attributes
    kind1 = MagicMock()
    kind1.key.name = "users"
    kind2 = MagicMock()
    kind2.key.name = "tasks"
    kind3 = MagicMock()
    kind3.key.name = "__internal__"  # Should be filtered out

    d._client.query.return_value.fetch.return_value = [kind1, kind2, kind3]

    result = d.get_table_names(None)
    assert "users" in result
    assert "tasks" in result
    assert "__internal__" not in result


def test_get_table_names_empty():
    d = CloudDatastoreDialect()
    d._client = MagicMock()
    d._client.query.return_value.fetch.return_value = []

    result = d.get_table_names(None)
    assert result == []
