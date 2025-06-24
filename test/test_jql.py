import subprocess
import json
import pandas as pd

project_id = "test-api-2"
credentials_path = "test_credentials.json"
gql_query = "SELECT * FROM APIKey;"

cmd = [
    "java", "-jar", "target/gql-fatjar.jar",
    project_id,
    credentials_path,
    gql_query 
]

res = subprocess.run(cmd, capture_output=True, text=True)
df = pd.DataFrame([json.loads(line) for line in res.stdout.strip().splitlines()])
print(df.head())
