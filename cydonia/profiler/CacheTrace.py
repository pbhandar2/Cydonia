"""CacheTrace generates a trace of fixed-sized block requests to cache from a multi-block storage trace. 

Usage:
    cache_trace = CacheTrace(block_storage_trace_path)
    cache_trace.write_to_file(cache_trace_path)
"""
from subprocess import Popen, PIPE 

from cydonia.profiler.CPReader import CPReader


class CacheTrace:
    def __init__(
            self, 
            stack_compute_binary_path: str 
    ) -> None:
        """Create a class to generate a trace pf fixed-sized block requests from multi-block storage trace.
        
        Args:
            _stack_binary_path: Path to the binary that compute stack distance. 
        """
        self._stack_binary_path = stack_compute_binary_path
    

    def generate_cache_trace(
            self, 
            block_trace_path: str,
            cache_trace_path: str 
    ) -> None:
        """Generate a cache trace at the specified path from the multi-block storage trace of this class.
        
        Args:
            cache_trace_path: Path to cache trace. 
        """
        reader = CPReader(block_trace_path)
        block_req_arr = reader.get_block_req_arr()        
        
        process = Popen([self._stack_binary_path], stdin=PIPE, stdout=PIPE)
        stdout = process.communicate(input="\n".join([str(_) for _ in block_req_arr]).encode("utf-8"))[0]
        rd_arr = stdout.decode("utf-8").split("\n")

        reader.generate_block_req_trace(rd_arr, cache_trace_path)


    def generate_access_trace(
            self, 
            block_trace_path: str,
            cache_trace_path: str 
    ) -> None:
        """Generate a cache trace at the specified path from the multi-block storage trace of this class.
        
        Args:
            cache_trace_path: Path to cache trace. 
        """
        reader = CPReader(block_trace_path)
        block_req_arr = reader.get_block_req_arr_without_misalignment()       
        
        process = Popen([self._stack_binary_path], stdin=PIPE, stdout=PIPE)
        stdout = process.communicate(input="\n".join([str(_) for _ in block_req_arr]).encode("utf-8"))[0]
        rd_arr = stdout.decode("utf-8").split("\n")

        reader.generate_block_req_trace_without_alignment(rd_arr, cache_trace_path)
