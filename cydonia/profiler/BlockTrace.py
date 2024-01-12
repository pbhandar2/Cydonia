from pathlib import Path 
from typing import Union
from dataclasses import dataclass
from time import perf_counter_ns
from pandas import DataFrame, read_csv 

from cydonia.profiler.WorkloadStats import WorkloadStats, BlockRequest 


@dataclass(frozen=True)
class ReaderConfig:
    ts_i: int = 0
    lba_i: int = 1
    op_i: int = 2 
    size_i: int = 3
    ts_unit: str = "us"
    lba_size_byte: int = 512
    cache_block_size_byte: int = 4096
    delimiter: str = ','
    read_str: str = 'r'
    write_str: str = 'w'
    ts_header_name: str = "ts"
    lba_header_name: str = "lba"
    op_header_name: str = "op"
    size_header_name: str = "size"
    iat_header_name: str = "iat"
    req_index_header_name: str = "i"
    cache_addr_header_name: str = "addr"
    front_misalign_header_name: str = "front_misalign"
    rear_misalign_header_name: str = "rear_misalign"
    scaled_cache_addr_header_name = "scaled_addr"


    def get_write_flag(self, op_str):
        if op_str == self.write_str:
            return True 
        elif op_str == self.read_str:
            return False 
        else:
            raise ValueError("Unrecognized operation string {}, allowed {} and {}.".format(op_str, self.op_type.READ, self.op_type.WRITE))
    

    def get_block_trace_header(self):
        header_name_arr = [''] * 4
        header_name_arr[self.ts_i] = self.ts_header_name
        header_name_arr[self.lba_i] = self.lba_header_name
        header_name_arr[self.op_i] = self.op_header_name
        header_name_arr[self.size_i] = self.size_header_name
        return header_name_arr


    def get_cache_trace_header(self):
        return [self.req_index_header_name, 
                    self.iat_header_name, 
                    self.cache_addr_header_name, 
                    self.op_header_name, 
                    self.front_misalign_header_name, 
                    self.rear_misalign_header_name]
    

class BlockTrace:
    """ BlockTrace reads block storage traces. 

    Attributes:
        _trace_file_path : Path object/string to the trace to read.
        _trace_file_handle : Handle to read the trace file.
        _config: ReaderConfig to read the block trace.
    """
    def __init__(
            self, 
            trace_file_path: Union[str, Path],
            config: ReaderConfig = ReaderConfig()
    ) -> None:
        """
        Args:
            trace_file_path : Path object/string to the trace to read.
            config: ReaderConfig to read the block trace.
        """
        self._trace_file_path = Path(trace_file_path)
        self._config = config
        self._df = self.load_block_trace(self._trace_file_path)


    def load_block_trace(self, block_trace_path):
        return self.add_iat_column(read_csv(block_trace_path, names=self._config.get_block_trace_header()), self._config)
    

    @staticmethod
    def add_iat_column(block_trace_df: DataFrame, config: ReaderConfig):
        block_trace_df[config.iat_header_name] = block_trace_df[config.ts_header_name] - block_trace_df[config.ts_header_name].shift(1).fillna(block_trace_df[config.ts_header_name][0])
        return block_trace_df
        
    
    def get_block_stat(self) -> WorkloadStats:
        assert self._config.iat_header_name in self._df and \
                self._config.lba_header_name in self._df and \
                    self._config.size_header_name in self._df and \
                        self._config.op_header_name in self._df
        
        workload_stat = WorkloadStats()
        for _, row in self._df.iterrows():
            cur_block_req = BlockRequest(row[self._config.ts_header_name],
                                            row[self._config.lba_header_name],
                                            self._config.get_write_flag(row[self._config.op_header_name]),
                                            row[self._config.size_header_name])
            workload_stat.track(cur_block_req)
        return workload_stat
