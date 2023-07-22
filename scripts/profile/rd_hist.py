import heapq
import pandas as pd 
from pathlib import Path 
from argparse import ArgumentParser 
from collections import Counter 


INFINITE_RD_VAL = 9223372036854775807


def get_max_rd(
    rd_counter: Counter
) -> int:
    """Get maximum reuse distance that is not 'INFINITE_RD_VAL' in the counter. 
    
    Args:
        rd_counter: Counter of reuse distance 
    
    Return:
        max_rd: Maximum reuse distance that is not 'INFINITE_RD_VAL'. 
    """
    top_2_rd = heapq.nlargest(2, rd_counter.keys())

    """The maximum possible value of RD is 'INFINITE_RD_VAL'. This value is not necessarily 
    a key in counter 'rd_counter'. We have to filter out this value if it exists in the top-2 values."""
    max_rd_list = [rd for rd in top_2_rd if rd != INFINITE_RD_VAL]
    return 0 if len(max_rd_list) == 0 else max_rd_list[0]


def generate_rd_hist(
        block_access_trace_path: str, 
        rd_hist_file_path: str 
    ) -> None:
        """ Generate RD histogram file given a block access trace file. 

        Args:
            block_access_trace_path: Path to block access trace. 
            rd_hist_file_path: Path where the RD histogram file will be generated. 
        """
        df = pd.read_csv(block_access_trace_path, names=['ts', 'block', 'op', 'rd'])

        read_rd_value_counts = df[df['op'] == 'r']['rd'].value_counts()
        write_rd_value_counts = df[df['op'] == 'w']['rd'].value_counts()
        max_rd = max(get_max_rd(read_rd_value_counts), get_max_rd(write_rd_value_counts))
        
        with open(rd_hist_file_path, "w+") as rd_hist_handle:
            for rd in range(-1, max_rd+1):
                if rd == -1:
                    cur_rd_read_count = read_rd_value_counts[INFINITE_RD_VAL]
                    cur_rd_write_count = write_rd_value_counts[INFINITE_RD_VAL]
                else:
                    cur_rd_read_count = read_rd_value_counts[rd] if rd in read_rd_value_counts else 0 
                    cur_rd_write_count = write_rd_value_counts[rd] if rd in write_rd_value_counts else 0 
                
                rd_hist_handle.write("{},{}\n".format(cur_rd_read_count, cur_rd_write_count))

        
if __name__ == "__main__":
    parser = ArgumentParser(description="Generate reuse distance histogram from a block access trace.")

    parser.add_argument("block_access_trace_path",
                        type=Path,
                        default="Path to a block access trace path.")
    
    parser.add_argument("rd_hist_file_path",
                        type=Path,
                        default="Path to file with read/write reuse distance histogram.")
    
    args = parser.parse_args()
    generate_rd_hist(args.block_access_trace_path, args.rd_hist_file_path)