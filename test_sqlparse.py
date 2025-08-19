import sqlparse

# Split a string containing two SQL statements:
raw = """
SELECT
                name AS name,
                age AS age,
                country AS country,
                create_time AS create_time,
                description AS description
            FROM (
                SELECT * FROM users
                ) AS virtual_table
            LIMIT 10'
"""
statements = sqlparse.split(raw)
print(statements)

# Format the first statement and print it out:
first = statements[0]
print(sqlparse.format(first, reindent=True, keyword_case="upper"))

# Parsing a SQL statement:
parsed = sqlparse.parse(raw)[0]
print(parsed.tokens)

raw2 = """
WITH employee_ranking AS (
  SELECT
    employee_id,
    last_name,
    first_name,
    salary,
    NTILE(2) OVER (ORDER BY salary ) as ntile
  FROM employee
)
SELECT
  employee_id,
  last_name,
  first_name,
  salary
FROM employee_ranking
WHERE ntile = 1
ORDER BY salary
"""
statements = sqlparse.split(raw2)
print(statements)

# Format the first statement and print it out:
first = statements[0]
print(sqlparse.format(first, reindent=True, keyword_case="upper"))

# Parsing a SQL statement:
parsed = sqlparse.parse(raw2)[0]
print(parsed.tokens)

raw4 = """
SELECT 
  task AS task, 
  count(DISTINCT rewards) AS "COUNT_DISTINCT(rewards)" 
  FROM 
    (SELECT * from users) 
	AS virtual_table GROUP BY description 
  ORDER BY "COUNT_DISTINCT(rewards)" D
  ESC LIMIT 10
"""

# Parsing a SQL statement:
parsed = sqlparse.parse(raw4)[0]
print(parsed.tokens)

raw5 = """
          SELECT
              name AS name,
              age AS age,
              country AS country,
              create_time AS create_time,
              description AS description
          FROM (
              SELECT * FROM users
              ) AS virtual_table
          LIMIT 10
          """
parsed = sqlparse.parse(raw5)[0]
print(parsed.tokens)
