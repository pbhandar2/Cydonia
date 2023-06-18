import pathlib 
import math 
import pandas as pd 

SECTOR_SIZE_BYTE = 512
BLOCK_SIZE_BYTE = 4096


if __name__ == "__main__":
    cp_data_dir = pathlib.Path("/research2/mtc/cp_traces/csv_traces")

    row_json_list = []
    for cp_trace_path in cp_data_dir.iterdir():
        print("Processing {}".format(cp_trace_path))
        trace_df = pd.read_csv(cp_trace_path, names=['ts', 'lba', 'op', 'size'])

        min_lba = trace_df['lba'].min() 
        max_lba = trace_df['lba'].max() 
        lba_range = max_lba - min_lba 

        min_offset = int((min_lba * SECTOR_SIZE_BYTE)/BLOCK_SIZE_BYTE) * BLOCK_SIZE_BYTE
        max_offset = int(((max_lba * SECTOR_SIZE_BYTE)/BLOCK_SIZE_BYTE) * BLOCK_SIZE_BYTE) + BLOCK_SIZE_BYTE

        range_gb = (max_offset - min_offset)/1e9

        row_json_list.append({
            "workload": cp_trace_path.stem,
            "min_offset": min_offset,
            "range_gb": math.ceil(range_gb)
        })
        print(row_json_list[-1])
    
    df = pd.DataFrame(row_json_list)
    df.to_csv("./data/cp_range.csv", index=False)

