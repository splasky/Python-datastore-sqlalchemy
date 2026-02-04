SQLAlchemy dialect for google cloud datastore(firestore mode)
========================
How to install
```
python3 setup.py install
```
or
```
pip install python-datastore-sqlalchemy
```
How to use
```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
engine = create_engine('datastore://test-api-1', credentials='path/to/credentials.json')
conn = engine.connect()
result = conn.execute("SELECT * from test_table)
print(result.fetchall())
```

## Preview
<img src="assets/pie.png">

> [!WARNING]
> Please note: Not all GQL and SQL syntax has been fully tested. Should you encounter any bugs, please post the relevant query and open an issue on GitHub.

## How to contribute
Feel free to open issues and pull requests on GitHub.

## Development Notes
- [Develop a SQLAlchemy and it's dialects](https://hackmd.io/lsBW5GCVR82SORyWZ1cssA?view)
