from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column

engine = create_engine('sqlite:///./my_users.db', echo=True) # Uncomment for file-based DB
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    age: Mapped[int] = mapped_column(Integer)

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', age={self.age})>"

Base.metadata.create_all(engine)


print("\n--- 插入資料 ---")
Session = sessionmaker(bind=engine)

with Session.begin() as session:
    user1 = User(name='Alice', age=30)
    user2 = User(name='Bob', age=24)
    user3 = User(name='Carol', age=35)

    session.add(user1)
    session.add_all([user2, user3]) # 可以一次添加多個

with engine.connect() as conn:
    result = conn.execute(text("SELECT id, name, age FROM users"))
    
    data_raw_sql = result.all()
    print("原始 SQL 查詢結果:")
    for row in data_raw_sql:
        print(f"ID: {row.id}, Name: {row.name}, Age: {row.age}")
