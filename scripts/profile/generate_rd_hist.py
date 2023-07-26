import pandas as pd 
from pathlib import Path 
from argparse import ArgumentParser 

def generate_rd_hist(
        block_access_trace_path, 
        rd_hist_file_path
    ) -> None:
        df = pd.read_csv(block_access_trace_path, names=['ts', 'block', 'op', 'rd'])
        read_rd_value_counts = df[df['op'] == 'r']['rd'].value_counts()
        write_rd_value_counts = df[df['op'] == 'w']['rd'].value_counts()

        print(read_rd_value_counts)
        print(write_rd_value_counts)


if __name__ == "__main__":
    parser = ArgumentParser(description="Generate reuse distance histogram from a block access trace.")

    parser.add_argument("block_access_trace_path",
                        type=Path,
                        default="Path to a block access trace path.")
    
    parser.add_argument("rd_hist_file_path",
                        type=Path,
                        default="Path to file with read/write reuse distance histogram.")
    
    args = parser.parse_args()

