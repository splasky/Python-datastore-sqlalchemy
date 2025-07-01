import subprocess
import pyarrow.ipc as pa_ipc

proc = subprocess.Popen(
    ["java", "-jar", "target/gql-query.jar", "test-api-2", "./test_credentials.json", "SELECT name, email FROM APIKey"],
    stdout=subprocess.PIPE
)

reader = pa_ipc.open_stream(proc.stdout)
table = reader.read_all()

df = table.to_pandas()
print(df.head())