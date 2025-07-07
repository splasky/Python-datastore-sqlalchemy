from sqlalchemy import create_engine, text
from sqlalchemy import Column, Integer, String, ARRAY
from sqlalchemy.orm import declarative_base

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./test_credentials.json"

# engine = create_engine('datastore://test-api-2')
# print("Dialect name:", engine.dialect.name)

# Base = declarative_base()
# engine = create_engine("datastore://test-api-2", echo=True)


# class APIKey(Base):
#     __tablename__ = "APIKey"  # This will be the 'kind' in Datastore
#     id = Column(Integer, primary_key=True)  # Datastore ID will map here
#     access = Column(ARRAY(String))  # Array of strings for access
#     counts = Column(Integer)
#     description = Column(String)
#     value = Column(String)

#     def __repr__(self):
#         return f"<APIKey(id={self.id}, access='{self.access}', counts='{self.counts}', description='{self.description}', value='{self.value}')>"


# Example of usage:
# from sqlalchemy.orm import sessionmaker

# Session = sessionmaker(bind=engine)
# session = Session()

# Insert
# new_user = User(id=1, name='Alice', email='alice@example.com') # ID can be provided or auto-generated
# session.add(new_user)
# session.commit()
# print(f"Inserted: {new_user}")

# # Query
# apikey = session.query(APIKey).filter_by(id=4857456767270912).first()
# print(f"Found: {apikey}")

# # Update
# if user:
#     user.email = 'alice.updated@example.com'
#     session.commit()
#     print(f"Updated: {user}")

# # Delete
# if user:
#     session.delete(user)
#     session.commit()
#     print(f"Deleted user with ID: {user.id}")

# # Drop kind (DANGEROUS: deletes all entities of 'User' kind)
# # Base.metadata.tables['User'].drop(engine)

# with engine.connect() as conn:
#     result = conn.execute(text("SELECT * FROM APIKey"))
#     for row in result:
#         print(row)
