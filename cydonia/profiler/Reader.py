from pathlib import Path 
from typing import Union
from abc import ABC, abstractmethod


class Reader(ABC):
    """The abstract Reader class that reads block storage traces. 

    Attributes:
        trace_file_path : Path object of the trace file. 
        trace_file_handle : Handle of the open trace file.
    """
    def __init__(
            self, 
            trace_file_path: Union[str, Path] 
    ) -> None:
        """
        Args:
            trace_file_path : Path object or path string to the trace to read.
        """
        self.trace_file_path = Path(trace_file_path)
        self.trace_file_handle = open(trace_file_path, "r")
    

    @abstractmethod
    def get_next_block_req(self):
        pass


    def generate_cache_trace(
            self, 
            cache_trace_path: Union[Path, str], 
            block_size_byte: int, 
            lba_size_byte: int 
    ) -> None:
        """Generate a cache trace file from the block trace file. 

        Args:
            cache_trace_path: Path object or path string pointing to the cache trace.
            block_size_byte: Size of a cache block.
            lba_size_byte: Size of a LBA(Logical Block Address) in the block trace.
        """

        with Path(cache_trace_path).open("w+") as f:
            block_req = self.get_next_block_req()
            while block_req:
                start_offset = block_req["lba"]*lba_size_byte
                end_offset = start_offset + block_req["size"] - 1
                start_page = start_offset//block_size_byte
                end_page = end_offset//block_size_byte
                for page_index in range(start_page, end_page+1):
                    f.write("{},{},{}\n".format(page_index, block_req["op"], block_req["ts"]))
                block_req = self.get_next_block_req()


    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.trace_file_handle.close()