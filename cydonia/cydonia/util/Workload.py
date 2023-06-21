import numpy as np 
import pandas as pd 

from collections import Counter


def get_working_set_size_byte(block_trace_path, lba_size_byte=512):
    block_acceess_counter = Counter()
    block_trace_df = pd.read_csv(block_trace_path, names=['ts', 'lba', 'op', 'size'])
    for row_index, row in block_trace_df.iterrows():
        start_lba = row['lba']
        end_lba = row['lba'] + int(np.floor(row['size']/lba_size_byte))
        for cur_lba in range(start_lba, end_lba + 1):
            block_acceess_counter[cur_lba] += 1
    
    unique_lba_count = len(block_acceess_counter.keys())
    return unique_lba_count * lba_size_byte