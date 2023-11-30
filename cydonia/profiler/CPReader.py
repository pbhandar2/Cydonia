import numpy as np 
from pathlib import Path 
from cydonia.profiler.Reader import Reader


KEY_LIST = ["ts", "lba", "op", "size"]


class CPReader(Reader): 
    """A reader class for cloudphysics block traces in CSV format."""

    def __init__(self, trace_path):
        super().__init__(trace_path)
        self.key_list = KEY_LIST
        self.cur_block_req = {}  
        self.start_time_ts = None 
        self.lba_size_byte = 512
        self.time_store = "relative"


    def get_next_block_req(self, **kwargs):
        """ Return a dict of block request attributes

        Return 
        ------
        block_req : dict 
            dict with block request attributes and values 
        """
        
        line = self.trace_file_handle.readline().rstrip()
        block_req = {}
        if line:
            split_line = line.split(",")

            if self.start_time_ts == None:
                self.start_time_ts = int(split_line[0])

            if self.time_store == "relative":
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
                block_req["start_block"] = (block_req["lba"] * self.lba_size_byte)//block_size
                block_req["key"] = block_req["start_block"]
                block_req["block_start_offset"] = block_req["start_block"] * block_size 
                block_req["end_block"] = (block_req["end_offset"]-1)//block_size 
                block_req["block_end_offset"] = (block_req["end_block"]+1) * block_size 
                block_req["front_misalign"] = block_req["start_offset"] - block_req["block_start_offset"]
                block_req["rear_misalign"] = block_req["block_end_offset"] - block_req["end_offset"] 

            if block_req and self.cur_block_req:
                assert(block_req["ts"] >= self.cur_block_req["ts"])

        self.cur_block_req = block_req
        return block_req

    
    def reset(self):
        """ Reset the file handle of the trace to the beginning and class
            attributes. 
        """

        self.trace_file_handle.seek(0)
        self.cur_block_req = {}


    def merge(self, reader2, output_path):
        """ Merge the trace from two readers ordered by the timestamp to create a new 
            trace file. 

        Parameters
        ----------
        reader2 : CPReader 
            CPReader with the trace to be combined with the trace of this reader 
        output_path : str 
            the path of the combined trace 
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
    

    def get_block_req_arr(
            self, 
            block_size_byte: int = 4096
    ) -> list:
        """Get the array of fixed-sized block accesses from block storage trace. 

        Args:
            block_size_byte: Size of block in bytes. 
        """
        self.reset()
        block_req_arr = []
        block_req = self.get_next_block_req(block_size=block_size_byte)
        while block_req:
            if block_req["op"] == 'r':
                for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                    block_req_arr.append(block_key)
            else:
                if block_req["start_block"] == block_req["end_block"]:
                    if block_req["front_misalign"] > 0 or block_req["rear_misalign"] > 0:
                        block_req_arr.append(block_req["start_block"])
                else:
                    if block_req["front_misalign"] > 0:
                        block_req_arr.append(block_req["start_block"])
                    
                    if block_req["rear_misalign"] > 0:
                        block_req_arr.append(block_req["end_block"])

                for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                    block_req_arr.append(block_key)
                    
            block_req = self.get_next_block_req(block_size=block_size_byte)
        return block_req_arr
    

    def get_block_req_arr_without_misalignment(
            self, 
            block_size_byte: int = 4096  
    ) -> list:
        self.reset()
        block_req_arr = []
        block_req = self.get_next_block_req(block_size=block_size_byte)
        while block_req:
            for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                block_req_arr.append(block_key)
            block_req = self.get_next_block_req(block_size=block_size_byte)
        return block_req_arr


    def generate_block_req_trace(
            self,
            rd_arr: list,
            block_req_trace_path: str,
            block_size_byte: int = 4096
    ) -> None:
        """Generate a block req trace given the reuse distance of each block request to cache. 
        
        Args:
            rd_arr: Array of reuse distance of each block request to cache. 
            block_req_trace_path: Path to block request trace. 
            block_size_byte: Size of block in bytes. 
        """
        self.reset()
        block_req_trace_handle = open(block_req_trace_path, "w+")
        block_req_count = 0 
        block_req = self.get_next_block_req(block_size=block_size_byte)
        while block_req:
            block_ts, block_op = block_req["ts"], block_req["op"]
            if block_op == 'r':
                for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                    block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_key, block_op, rd_arr[block_req_count]))
                    block_req_count += 1
            else:
                if block_req["start_block"] == block_req["end_block"]:
                    if block_req["front_misalign"] > 0 or block_req["rear_misalign"] > 0:
                        block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_req["start_block"], 'r', rd_arr[block_req_count]))
                        block_req_count += 1
                else:
                    if block_req["front_misalign"] > 0:
                        block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_req["start_block"], 'r', rd_arr[block_req_count]))
                        block_req_count += 1
                    
                    if block_req["rear_misalign"] > 0:
                        block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_req["end_block"], 'r', rd_arr[block_req_count]))
                        block_req_count += 1
                for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                        block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_key, block_op, rd_arr[block_req_count]))
                        block_req_count += 1
            block_req = self.get_next_block_req(block_size=block_size_byte)
        assert len(rd_arr) == (block_req_count + 1)
        block_req_trace_handle.close()


    def generate_block_req_trace_without_alignment(
            self,
            rd_arr: list,
            block_req_trace_path: str,
            block_size_byte: int = 4096
    ) -> None:
        """Generate a block req trace given the reuse distance of each block request to cache. 
        
        Args:
            rd_arr: Array of reuse distance of each block request to cache. 
            block_req_trace_path: Path to block request trace. 
            block_size_byte: Size of block in bytes. 
        """
        self.reset()
        block_req_trace_handle = open(block_req_trace_path, "w+")
        block_req_count = 0 
        block_req = self.get_next_block_req(block_size=block_size_byte)
        while block_req:
            block_ts, block_op = block_req["ts"], block_req["op"]
            for block_key in range(block_req["start_block"], block_req["end_block"]+1):
                block_req_trace_handle.write("{},{},{},{}\n".format(block_ts, block_key, block_op, rd_arr[block_req_count]))
                block_req_count += 1
            block_req = self.get_next_block_req(block_size=block_size_byte)
        assert len(rd_arr) == (block_req_count + 1)
        block_req_trace_handle.close()
    

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

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.trace_file_handle.close()