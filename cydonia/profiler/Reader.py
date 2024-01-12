""" The abstract Reader class that reads block storage traces. """

from enum import Enum
from dataclasses import dataclass

from pathlib import Path 
from typing import Union
from abc import ABC, abstractmethod


class OPTYPE(Enum):
    READ='r'
    WRITE='w'


@dataclass
class ReaderConfig:
    ts: int
    lba: int
    op: int
    size: int
    ts_unit: str = "us"
    lba_size_byte: int = 512
    cache_size_byte: int = 4096

    def get_cache_trace_header(self):
        return ["ts", "iat", "addr", "front", "rear"]


@dataclass 
class BlockRequest:
    ts: int
    lba: int
    size_byte: int 
    op: OPTYPE
    iat: int 
    trace_string: str 
    prev_ts: int = -1
    iat: int = -1 

    def __post_init__(self):
        if self.prev_ts == -1:
            self.iat = -1
        else:
            self.iat = self.ts - self.prev_ts
        self.trace_string = "{},{},{},{}\n".format(self.ts, self.lba, self.op.value, self.size_byte)
    

@dataclass
class CacheRequest:
    req_i: int
    iat: int 
    addr: int 
    op: OPTYPE
    front_misalign_byte: int 
    rear_misalign_byte: int 
    block_size: int

    def get_blockRequest_arr(self) -> list:
        pass 

    def __str__(self) -> str:
        return "{},{},{},{},{},{}\n".format(self.req_i, self.iat, self.addr, self.op.value, self.front_misalign_byte, self.rear_misalign_byte)

    

@dataclass
class ReaderStats:
    read_count: int = 0
    write_count: int = 0 
    read_bytes: int = 0
    write_bytes: int = 0 
    read_iat: int = 0 
    write_iat: int = 0 


class Reader(ABC):
    """The abstract Reader class that reads block storage traces. 

    Attributes:
        _trace_file_path : Path object/string to the trace to read.
        _trace_file_handle : Handle to read the trace file.
        _lba_size_byte: Size of an LBA (Logical Block Address) in the block trace.
    """
    def __init__(
            self, 
            trace_file_path: Union[str, Path],
            lba_size_byte: int = 512 
    ) -> None:
        """
        Args:
            trace_file_path : Path object/string to the trace to read.
            lba_size_byte: Size of an LBA (Logical Block Address) in the block trace.
        """
        self._trace_file_path = Path(trace_file_path)
        self._trace_file_handle = open(self._trace_file_path, "r")
        self._lba_size_byte = lba_size_byte


    @abstractmethod
    def get_next_block_req(self):
        pass


    @staticmethod
    def get_cache_access_features(
            lba, 
            lba_size_byte, 
            size_byte, 
            cache_block_size_byte
    ) -> list:
        """ Get the cache blocks accessed and the front and rear misalignment 
        due to a block request with the given parameters.

        Args:
            lba: LBA (Logical Block Address) of the block request.
            lba_size_byte: Size of an LBA. 
            size_byte: Size of the block request.
            cache_block_size_byte: Size of a cache block.
        
        Returns:
            cache_access_feature_arr: An array of start cache block address, end cache block address, front misalignment in bytes
                                        and rear misalignment in bytes. 
        """
        start_offset = lba*lba_size_byte
        end_offset = start_offset + size_byte - 1
        start_blk_addr = start_offset//cache_block_size_byte
        end_blk_addr = end_offset//cache_block_size_byte
        front_misalign_byte = start_offset - (start_blk_addr * cache_block_size_byte)
        rear_misalign_byte = ((end_blk_addr + 1) * cache_block_size_byte) - (start_offset + size_byte)
        return [start_blk_addr, end_blk_addr, front_misalign_byte, rear_misalign_byte]
    

    @staticmethod
    def get_blockRequest_arr(

    ) -> list:
        pass 


    def close(self):
        self._trace_file_handle.close()