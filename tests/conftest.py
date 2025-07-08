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
import pytest
import os

from sqlalchemy.dialects import registry
from sqlalchemy import create_engine

registry.register("datastore", "sqlalchemy_datastore", "DatastoreDialect")

# Fixture example (add this to your conftest.py)
@pytest.fixture
def conn():
    """Database connection fixture - implement according to your setup"""
    os.environ["DATASTORE_EMULATOR_HOST"]="localhost:8081"
    engine = create_engine('datastore://python-datastore-sqlalchemy', echo=True)
    conn = engine.connect()
    return conn

