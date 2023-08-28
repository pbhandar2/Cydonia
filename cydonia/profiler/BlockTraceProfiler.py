"""BlockTraceProfiler generates features from multi-block storage trace. 

Usage:
    reader = CPReader(block_trace_path)
    profiler = BlockTraceProfiler(reader)
    profiler.run() 
    stats = profiler.get_stat() 
"""

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockStorageTraceStats import BlockStorageTraceStats


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