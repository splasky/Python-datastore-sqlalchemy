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
from . import Base
from sqlalchemy import Column, Integer, String, JSON, Boolean, ARRAY, FLOAT, BINARY

class Task(Base):

    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True, autoincrement=True) 
    task = Column(String)
    content = Column(JSON)
    is_done = Column(Boolean)
    tag = Column(String)
    location = Column(ARRAY(FLOAT))
    assign_user = Column(JSON)
    reward = Column(FLOAT)
    equipment = Column(JSON)
    additional_notes = Column(JSON)
    encrypted_formula = Column(BINARY)

    def __repr__(self):
        return (f"<Task(id={self.id}, "
                f"task={self.task!r}, "
                f"is_done={self.is_done}, "
                f"tag={self.tag!r}, "
                f"reward={self.reward})>")
