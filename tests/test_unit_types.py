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
"""Unit tests for _types module (no emulator required)."""
import sqlalchemy.types
from google.cloud.bigquery.schema import SchemaField

from sqlalchemy_datastore._types import (
    ARRAY,
    BOOLEAN,
    BYTES,
    DATE,
    DATETIME,
    FLOAT,
    FLOAT64,
    GEOPOINT,
    INTEGER,
    KEY_TYPE,
    NULL_TYPE,
    NUMERIC,
    RECORD,
    STRING,
    STRUCT_FIELD_TYPES,
    TIME,
    TIMESTAMP,
    _get_sqla_column_type,
    _property_type,
    _type_map,
    get_columns,
)

# ---------------------------------------------------------------------------
# Type map completeness
# ---------------------------------------------------------------------------

def test_type_map_keys():
    expected_keys = {
        "ARRAY", "BIGNUMERIC", "BOOLEAN", "BOOL", "BYTES",
        "DATETIME", "DATE", "FLOAT64", "FLOAT", "INT64", "INTEGER",
        "NUMERIC", "RECORD", "STRING", "STRUCT", "TIMESTAMP", "TIME",
        "GEOGRAPHY", "NULL", "KEY",
    }
    assert set(_type_map.keys()) == expected_keys


def test_type_map_values_are_sqlalchemy_types():
    for key, val in _type_map.items():
        assert issubclass(val, sqlalchemy.types.TypeEngine), (
            f"{key} -> {val} is not a SQLAlchemy type"
        )


# ---------------------------------------------------------------------------
# Dialect-provided type aliases
# ---------------------------------------------------------------------------

def test_dialect_type_aliases():
    assert ARRAY is sqlalchemy.types.ARRAY
    assert BOOLEAN is sqlalchemy.types.Boolean
    assert BYTES is sqlalchemy.types.BINARY
    assert DATETIME is sqlalchemy.types.DATETIME
    assert DATE is sqlalchemy.types.DATE
    assert FLOAT64 is sqlalchemy.types.Float
    assert FLOAT is sqlalchemy.types.Float
    assert INTEGER is sqlalchemy.types.Integer
    assert NUMERIC is sqlalchemy.types.Numeric
    assert STRING is sqlalchemy.types.String
    assert TIMESTAMP is sqlalchemy.types.TIMESTAMP
    assert TIME is sqlalchemy.types.TIME
    assert GEOPOINT is sqlalchemy.types.JSON
    assert STRUCT_FIELD_TYPES is sqlalchemy.types.JSON
    assert RECORD is sqlalchemy.types.JSON
    assert NULL_TYPE is sqlalchemy.types.NullType
    assert KEY_TYPE is sqlalchemy.types.JSON


# ---------------------------------------------------------------------------
# _property_type mapping (datastore property types)
# ---------------------------------------------------------------------------

def test_property_type_mapping():
    assert isinstance(_property_type["Date/Time"], DATETIME)
    assert isinstance(_property_type["Integer"], INTEGER)
    assert isinstance(_property_type["Boolean"], BOOLEAN)
    assert isinstance(_property_type["String"], STRING)
    assert isinstance(_property_type["Float"], FLOAT)
    assert isinstance(_property_type["Key"], KEY_TYPE)
    assert isinstance(_property_type["Blob"], BYTES)
    assert isinstance(_property_type["NULL"], NULL_TYPE)
    assert isinstance(_property_type["EmbeddedEntity"], STRUCT_FIELD_TYPES)
    assert isinstance(_property_type["GeoPt"], GEOPOINT)


# ---------------------------------------------------------------------------
# _get_sqla_column_type (using real SchemaField objects)
# ---------------------------------------------------------------------------

def test_get_sqla_column_type_string():
    field = SchemaField("name", "STRING", max_length=255)
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.String)


def test_get_sqla_column_type_integer():
    field = SchemaField("age", "INTEGER")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.Integer)


def test_get_sqla_column_type_float():
    field = SchemaField("score", "FLOAT64")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.Float)


def test_get_sqla_column_type_boolean():
    field = SchemaField("active", "BOOLEAN")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.Boolean)


def test_get_sqla_column_type_timestamp():
    field = SchemaField("created", "TIMESTAMP")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.TIMESTAMP)


def test_get_sqla_column_type_date():
    field = SchemaField("dob", "DATE")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.DATE)


def test_get_sqla_column_type_datetime():
    field = SchemaField("ts", "DATETIME")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.DATETIME)


def test_get_sqla_column_type_time():
    field = SchemaField("clock", "TIME")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.TIME)


def test_get_sqla_column_type_bytes():
    field = SchemaField("data", "BYTES", max_length=1024)
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.BINARY)


def test_get_sqla_column_type_numeric():
    field = SchemaField("amount", "NUMERIC", precision=10, scale=2)
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.Numeric)


def test_get_sqla_column_type_bignumeric():
    field = SchemaField("big_amount", "BIGNUMERIC", precision=38, scale=9)
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.Numeric)


def test_get_sqla_column_type_null():
    field = SchemaField("x", "NULL")
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.NullType)


def test_get_sqla_column_type_repeated():
    field = SchemaField("tags", "STRING", mode="REPEATED", max_length=100)
    coltype = _get_sqla_column_type(field)
    assert isinstance(coltype, sqlalchemy.types.ARRAY)


def test_get_sqla_column_type_unknown():
    field = SchemaField("mystery", "UNKNOWN_TYPE_XYZ")
    coltype = _get_sqla_column_type(field)
    assert coltype is sqlalchemy.types.NullType


# ---------------------------------------------------------------------------
# get_columns
# ---------------------------------------------------------------------------

def test_get_columns_empty():
    columns = get_columns([])
    assert columns == []
