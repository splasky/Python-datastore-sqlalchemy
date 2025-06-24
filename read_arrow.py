import pyarrow as pa

# 假設你已經有一個 Arrow stream file 或 bytes 資料
with pa.OSFile('./output.arrow', 'rb') as source:
    loaded_array = pa.ipc.open_file(source).read_all()
    
    print(loaded_array)

    # 或逐批讀出來
    for batch in loaded_array:
        print(batch)
