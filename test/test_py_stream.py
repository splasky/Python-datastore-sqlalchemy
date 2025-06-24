import subprocess
import pyarrow.ipc as pa_ipc
import pandas as pd

cmd = [
    "java",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "-jar", "target/gql-query.jar",
    "test-api-2",
    "./credentials.json",
    "SELECT * FROM APIKey"
]

with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:
    reader = pa_ipc.open_stream(proc.stdout)
    table = reader.read_all()

df = table.to_pandas()
print(df.head())
