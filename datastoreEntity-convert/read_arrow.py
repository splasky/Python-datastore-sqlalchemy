#!/usr/bin/env python3
import pyarrow as pa
import pyarrow.ipc as ipc

# 打開並讀取 .arrow 檔案
with open("./output.arrow", "rb") as f:
    reader = ipc.RecordBatchFileReader(f)
    table = reader.read_all()

# 顯示內容
print(table)
print(table.to_pandas())  # 如果你想轉成 pandas DataFrame
