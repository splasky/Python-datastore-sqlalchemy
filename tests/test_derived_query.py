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
from models.user import User
from sqlalchemy import select

def test_derived_query_orm(session):
    # Test the derived query
    # Step 1: Create the inner query (SELECT * FROM users)
    # In ORM, `select(User)` implies selecting all columns mapped to the User model.
    inner_query_statement = select(User)

    # Step 2: Create a subquery from the inner query and alias it as 'virtual_table'
    # .subquery() makes it a subquery, and .alias() gives it the name.
    virtual_table_alias = inner_query_statement.subquery().alias("virtual_table")

    # Step 3: Create the outer query, selecting specific columns from the aliased subquery.
    # `virtual_table_alias.c` provides access to the columns of the aliased subquery.
    orm_query_statement = select(
        virtual_table_alias.c.name,
        virtual_table_alias.c.age,
        virtual_table_alias.c.country,
        virtual_table_alias.c.create_time,
        virtual_table_alias.c.description
    ).limit(10) # Apply the LIMIT 10

    # Execute the ORM query using the session
    result = session.execute(orm_query_statement)
    data = result.fetchall() # Fetch all results

    # --- Assertions ---
    print(f"Fetched {len(data)} rows:")
    for row in data:
        print(row)
