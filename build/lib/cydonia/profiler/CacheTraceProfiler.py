"""CacheTraceProfiler generates features from a cache trace with format: 
timestamp, block_id, operation, reuse distance.

Usage:
    profiler = CacheTraceProfiler(block_trace_path)
    profiler.create_rd_hist_file(rd_hist_file_path)
"""
from pathlib import Path 
from pandas import read_csv 

from cydonia.profiler.RDHistogram import RDHistogram


class CacheTraceProfiler:
    def __init__(
            self, 
            block_trace_path: str
    ) -> None:
        self.df = read_csv(block_trace_path, names=["ts", "id", "op", "rd"])
    

    def create_rd_hist_file(
            self, 
            rd_hist_file_path: Path
    ) -> None:
        """Create RD histogram file from a cache trace. 
        
        Args:
            rd_hist_file_path: Path to reuse distance histogram file to be created. 
        """
        rd_hist = RDHistogram()
        len_df = len(self.df)
        for row_index, row in self.df.iterrows():
            if row_index % 1000000 == 0:
                print("Processed {}, {}% requests!".format(row_index, row_index/len_df)) 
            op, rd = row["op"], row["rd"]
            rd_hist.update_rd(rd, op)
        rd_hist.write_to_file(rd_hist_file_path)
    
