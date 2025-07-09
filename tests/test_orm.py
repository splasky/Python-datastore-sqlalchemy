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
from datetime import datetime, timezone
from models.user import User


def test_user_crud(session):
    user_info = {
        "name":"因幡めぐる",
        "age":float('nan'), # or 16
        "country":"Japan",
        "create_time": datetime(2025, 1, 1, 1, 2, 3, 4, tzinfo=timezone.utc),
        "description": "Ciallo～(∠・ω< )⌒☆",
        "settings":{
            "team": "超自然研究部",
            "grade": 10, # 10th grade
            "birthday": "04-18",
            "school": "姬松學園",
        },
    }
    # Create
    user = User(
        name=user_info["name"],
        age=user_info["age"], # or 16
        country=user_info["country"],
        create_time=user_info["create_time"],
        description=user_info["description"],
        settings={
            "team": user_info["settings"]["settings"],
            "grade": user_info["settings"]["grade"], # 10th grade
            "birthday": user_info["settings"]["birthday"],
            "school": user_info["settings"]["school"],
        },
    )
    user_id = user.id
    session.add(user)
    session.commit()

    # Read
    result = session.query(User).filter_by(id=user_id).first()
    assert result is not None
    assert result.name == user_info["name"]
    assert result.age == user_info["age"]
    assert result.country == user_info["country"]
    assert result.create_time == user_info["create_time"]
    assert result.description == user_info["description"]
    assert result.settings == user_info["settings"]

    # Update
    result.age = 16
    session.commit()

    updated = session.query(User).filter_by(id=user_id).first()
    assert updated.value == 16

    # Delete
    user_id = user_id
    session.delete(updated)
    session.commit()

    deleted = session.query(user).filter_by(id=user_id).first()
    assert deleted is None
