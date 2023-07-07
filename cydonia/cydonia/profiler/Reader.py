import pathlib 
from abc import ABC, abstractmethod

class Reader(ABC):
    """
    The abstract Reader class

    ...

    Attributes
    ----------
    trace_file_path : pathlib.Path
        Path object of the block trace path  
    trace_file_handle : File/handle 
        File object of the trace file 
    
    Methods
    -------
    get_next_block_req(self)
        Get JSON object comprising of attributes and values of the next block request 
    generate_page_trace(self, page_trace_path, page_size, block_size)
        Generate a page trace from the block file for a specified block and page size 
    """

    def __init__(self, trace_file_path):
        """
        Parameters
        ----------
        trace_file_path : str 
            block trace file path 
        """

        self.trace_file_path = pathlib.Path(trace_file_path)
        self.trace_file_handle = open(trace_file_path, "r")
    

    @abstractmethod
    def get_next_block_req(self):
        pass


    def generate_page_trace(self, page_trace_path, page_size, block_size):
        """ Generate a page trace file from the block trace file 

        Parameters
        ----------
        page_trace_path : str 
            page trace file path 
        page_size : int 
            size of a page in bytes of the page trace 
        block_size : int 
            size of a block/LBA in bytes of the block trace 
        """

        with pathlib.Path(page_trace_path).open("w+") as f:
            block_req = self.get_next_block_req()
            while block_req:
                start_offset = block_req["lba"]*block_size 
                end_offset = start_offset + block_req["size"] - 1
                start_page = start_offset//page_size
                end_page = end_offset//page_size
                for page_index in range(start_page, end_page+1):
                    f.write("{},{},{}\n".format(page_index, block_req["op"], block_req["ts"]))
                block_req = self.get_next_block_req()


    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.trace_file_handle.close()