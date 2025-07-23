Dialets for SQLAlchemy
========================
How to install
```
pip install sqlalchemy-gcp-datastore
```
SQLAlchemy
```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
engine = create_engine('datastore://test-api-1', credentials='path/to/credentials.json')
conn = engine.connect()
result = conn.execute("SELECT * from test_table)
print(result.fetchall())
```
