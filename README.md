Dialets for SQLAlchemy
========================
Installation
```
pip install sqlalchemy-gcp-datastore
```
SQLAlchemy
```
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
engine = create_engine('datastore://project', credentials='path/to/credentials.json')
table = Table('dataset.table', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=table().scalar()))
```
