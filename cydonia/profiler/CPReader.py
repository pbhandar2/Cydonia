"""The reader class to read and process CSV block traces with format: ts, lba, op, size."""

import numpy as np 
from pathlib import Path 
from cydonia.profiler.Reader import Reader, get_cache_access_features 


KEY_LIST = ["ts", "lba", "op", "size"]


class CPReader(Reader):
    """The reader class to read and process CSV block traces with format: ts, lba, op, size.
    
    Attributes:
        key_list: List of keys in a dictionary of block request. 
        cur_block_req: Dictionary with attributes of the most recently read block request. 
        start_time_ts: The start time timestamp of the block trace. 
        lba_size_byte: The size of a sector or logical block address in bytes. 
        _relative_time_flag: Boolean flag indicating if relative time should be used instead of absolute time. 
    """
    def __init__(
            self, 
            trace_path: Path 
    ) -> None:
        """
        Args:
            trace_path: Path to the block trace to read.
        """
        super().__init__(trace_path)
        self.key_list = KEY_LIST
        self.cur_block_req = {}  
        self.start_time_ts = None 
        self.lba_size_byte = 512
        self._relative_time_flag = True


    def get_next_block_req(
            self, 
            **kwargs: dict
    ) -> dict:
        """Return a dictionary with attributes of the next block request.

        Args:
            kwargs: Dictionary of keyword arguments. 

        Returns:
            block_req: Dictionary with block request attributes and values 
        """
        
        line = self._trace_file_handle.readline().rstrip()
        block_req = {}
        if line:
            split_line = line.split(",")

            if self.start_time_ts == None:
                self.start_time_ts = int(split_line[0])

            if self._relative_time_flag:
                block_req["ts"] = int(split_line[0]) - self.start_time_ts
            else:
                block_req["ts"] = int(split_line[0])
            
            block_req["lba"] = int(split_line[1])
            block_req["op"] = split_line[2]
            block_req["size"] = int(split_line[3])
            block_req["start_offset"] = block_req["lba"] * self.lba_size_byte
            block_req["end_offset"] = block_req["start_offset"] + block_req["size"] 

            if 'block_size' in kwargs:
                block_size = int(kwargs['block_size'])
                cache_access_feature_tuple = get_cache_access_features(block_req["lba"], self.lba_size_byte, block_req["size"], block_size)
                block_req["start_block"], block_req["end_block"], block_req["front_misalign"], block_req["rear_misalign"] = cache_access_feature_tuple
                assert block_req["front_misalign"] < block_size and block_req["rear_misalign"] < block_size, \
                        "The misalignment cannot be greater than or equal to the cache block size, but found {} and {} with block size {}.".format(block_req["front_misalign"], block_req["rear_misalign"], block_size)

            if block_req and self.cur_block_req:
                assert(block_req["ts"] >= self.cur_block_req["ts"], \
                        "Timestamp of consequetive block request should be equal or greater, but found {} vs {}.".format(block_req["ts"], self.cur_block_req["ts"]))

        self.cur_block_req = block_req
        return block_req

    
    def reset(self):
        """Reset the file handle of the trace to the beginning and class attributes."""
        self._trace_file_handle.seek(0)
        self.cur_block_req = {}


    def merge(self, 
                reader2: Reader, 
                output_path: Path
    ) -> None:
        """Merge the trace from two readers ordered by the timestamp to create a new trace file. 

        Args:
            reader2: CPReader with the trace to be combined with the trace of this reader. 
            output_path: Output path of the combined trace. 
        """
        self.reset()
        reader2.reset()
        out_handle = open(output_path, "w+")

        reader1_req = self.get_next_block_req()
        reader2_req = reader2.get_next_block_req()
        while reader1_req or reader2_req:
            reader1_start_time = reader1_req["ts"]-self.start_time_ts if reader1_req else np.inf 
            reader2_start_time = reader2_req["ts"]-reader2.start_time_ts if reader2_req else np.inf 

            if reader1_start_time < reader2_start_time:
                out_str_array = []
                for index in range(len(self.key_list)):
                    k = self.key_list[index]
                    if k == "lba":
                        out_str_array.append("2{}".format(str(reader1_req[k])))
                    elif k == "ts":
                        out_str_array.append(str(reader1_req["ts"]-self.start_time_ts))
                    else:
                        out_str_array.append(str(reader1_req[k]))
                out_handle.write("{}\n".format(",".join(out_str_array)))
                reader1_req = self.get_next_block_req()
            else:
                out_str_array = []
                for index in range(len(self.key_list)):
                    k = self.key_list[index]
                    if k == "lba":
                        out_str_array.append("1{}".format(str(reader2_req[k])))
                    elif k == "ts":
                        out_str_array.append(str(reader2_req["ts"]-reader2.start_time_ts))
                    else:
                        out_str_array.append(str(reader2_req[k]))
                out_handle.write("{}\n".format(",".join(out_str_array)))
                reader2_req = reader2.get_next_block_req()

        out_handle.close()

    
    def generate_cache_trace(
            self, 
            cache_trace_path: Path, 
            block_size_byte: int = 4096
    ) -> None:
        """Generate cache trace from a block trace. 
        
        Args:
            cache_trace_path: Path to the new cache trace. 
            block_size_byte: Size of a block in cache. (Default: 4096)
        """
        self.reset()
        block_req_count = 0 
        cache_trace_handle = open(cache_trace_path, "w+")

        block_req = self.get_next_block_req(block_size=block_size_byte)
        prev_ts_us = block_req["ts"]
        """A cache trace has format: block_req_index, iat, block_key, op, front misalign, rear misalign. 
            - "block_req_index" identifies requests to cache that belong to the same block request. 
                This attribute is useful when converting a cache trace to a sample block trace. 
            - "iat" is interarrival time of the block request. All blocks related to the same block request share
                the same interarrival time. 
            - "block_key" is the key of the block accessed. 
            - "op" is the operation 
            - "front misalign" represents the misalignment in the first block. 
            - "rear misalign" represents the misalignment in the rear block. 
        """ 
        while block_req:
            block_req_count += 1
            block_iat_us = block_req["ts"] - prev_ts_us
            block_op = block_req["op"]
            block_count = block_req["end_block"] + 1 - block_req["start_block"]

            if block_count == 1:
                if block_op == 'w' and (block_req["front_misalign"] > 0 or block_req["rear_misalign"] > 0):
                    cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, 
                                                                            block_iat_us, 
                                                                            block_req["start_block"], 
                                                                            'r', 
                                                                            block_req["front_misalign"], 
                                                                            block_req["rear_misalign"]))
                        
                cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, 
                                                                        block_iat_us, 
                                                                        block_req["start_block"], 
                                                                        block_op, 
                                                                        block_req["front_misalign"], 
                                                                        block_req["rear_misalign"]))
            else:
                if block_op == 'w':
                    if block_req["front_misalign"] > 0:
                        cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, 
                                                                                block_iat_us, 
                                                                                block_req["start_block"], 
                                                                                'r', 
                                                                                block_req["front_misalign"], 
                                                                                0))

                    if block_req["rear_misalign"] > 0:
                        cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, 
                                                                                block_iat_us, 
                                                                                block_req["end_block"], 
                                                                                'r', 
                                                                                0, 
                                                                                block_req["rear_misalign"]))

                for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                    if block_key == block_req["start_block"]:
                        cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, block_iat_us, block_key, block_op, block_req["front_misalign"], 0))
                    elif block_key == block_req["end_block"]:
                        cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, block_iat_us, block_key, block_op, 0, block_req["rear_misalign"]))
                    else:
                        cache_trace_handle.write("{},{},{},{},{},{}\n".format(block_req_count, block_iat_us, block_key, block_op, 0, 0))

            prev_ts_us = block_req["ts"]
            block_req = self.get_next_block_req(block_size=block_size_byte)
        cache_trace_handle.close()