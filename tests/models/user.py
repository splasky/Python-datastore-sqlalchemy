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
from sqlalchemy import DATETIME, JSON, Column, Integer, String

from . import Base


class User(Base):

    __tablename__ = "users"  # This will be the 'kind' in Datastore
    id = Column(Integer, primary_key=True, autoincrement=True)  # Datastore ID will map here
    name = Column(String)  # Array of strings for access
    age = Column(Integer)
    country = Column(String)
    create_time = Column(DATETIME)
    description = Column(String)
    settings = Column(JSON)

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', age='{self.age}', country='{self.country}, create_time='{str(self.create_time)}', description='{self.description}', settings='{self.settings}')>"
