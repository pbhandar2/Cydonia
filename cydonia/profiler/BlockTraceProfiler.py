"""BlockTraceProfiler generates features from multi-block storage trace. 

Usage:
    reader = CPReader(block_trace_path)
    profiler = BlockTraceProfiler(reader)
    profiler.run() 
    stats = profiler.get_stat() 
"""

from typing import Union
from pathlib import Path 
from numpy import ndarray, array 
from pandas import read_csv 

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockStorageTraceStats import BlockStorageTraceStats


class TraceProfiler:
    def __init__(
            self,
            block_trace_path: Path 
    ) -> None:
        self._df = self.load_block_trace(block_trace_path)
    

    @staticmethod
    def load_block_trace(block_trace_path):
        block_trace_df = read_csv(block_trace_path, names=["ts", "lba", "op", "size"])
        block_trace_df["prev_ts"] = block_trace_df["ts"].shift(1).fillna(block_trace_df["ts"][0])
        block_trace_df["iat"] = block_trace_df["ts"] - block_trace_df["prev_ts"]
        return block_trace_df


def get_unique_block_arr(
        block_trace_path: Union[str, Path],
        block_size_byte: int
) -> ndarray:
    """Get a numpy array of all unique block address from a CP block trace.

    Args:
        block_trace_path: Path object or string pointing to the CP block trace.
        block_size_byte: Size of data block in bytes. Note that it is different from the size of the LBA
                            which is fixed to 512 bytes in CP block traces.
    
    Returns:
        unique_block_arr: Numpy array of unique block addresses in the CP block trace.
    """
    reader = CPReader(block_trace_path)
    unique_block_set = set()
    cur_block_req = reader.get_next_block_req(block_size=block_size_byte)
    while cur_block_req:
        for block_addr in range(cur_block_req["start_block"], cur_block_req["end_block"]+1):
            unique_block_set.add(block_addr)
        cur_block_req = reader.get_next_block_req(block_size=block_size_byte)
    return array(unique_block_set, dtype=int)


class BlockTraceProfiler:
    def __init__(
            self, 
            reader: CPReader
    ) -> None:
        """This class profiles block storage traces.
        
        Args:
            reader: Reader class to read the content of block storage trace. 
        """
        self._page_size = 4096
        self._reader = reader 
        self._workload_name = self._reader.trace_file_path.stem 

        self._stat = {} 
        self._stat['block'] = BlockStorageTraceStats()

        # track the latest and previous cache request 
        self._cur_req, self._prev_req = {}, {}
        
        # track the time elapsed 
        self._time_elasped = 0 


    def _load_next_block_req(self):
        """Load the next multi-block storage request to block storage system."""
        self._cur_req = self._reader.get_next_block_req(page_size=self._page_size) 
        if self._cur_req:
            self._time_elasped = self._cur_req["ts"]
            self._cur_req["key"] = self._cur_req["start_page"]
            self._stat['block'].add_request(self._cur_req)


    def _load_next_cache_req(self):
        """Load the next fixed-sized block request to cache."""
        if self._cur_req:
            self._prev_req = self._cur_req
        
        if not self._cur_req:
            self._load_next_block_req()
        else:
            if self._cur_req["key"] == self._cur_req["end_page"]:
                self._load_next_block_req()
            else:
                self._cur_req["key"] += 1
    

    def get_stat(self):
        """Get a dictionary containing features of the block storage trace."""
        return self._stat['block'].get_stat()


    def get_next_cache_req(self):
        """Get a dictionary with features of the next fixed-sized block request to cache."""
        self._load_next_cache_req()
        return self._cur_req
            

    def run(self):
        """ This function computes features from the provided trace. """
        self._reader.reset()
        self._load_next_cache_req()
        while (self._cur_req):
            self._load_next_cache_req()