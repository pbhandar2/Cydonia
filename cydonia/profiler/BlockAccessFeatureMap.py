"""BlockAccessFeatureMap stores the access features of each data block in a block trace. The size of
a data block can be defined by the user. It can compute the new workload feauture if a given block
were to be removed from the trace using the block access feature of the block and the current feature
of the trace. 

Usage:
    feature_map = BlockAccessFeatureMap()
    feature_map.load(block_trace_path)
"""

from enum import Enum
from numpy import zeros 
from pathlib import Path 
from copy import deepcopy
from time import perf_counter_ns

from cydonia.profiler.CPReader import CPReader


class MetadataIndex(Enum):
    MISALIGNMENT = 0
    SOLO = 1
    LEFT = 7
    RIGHT = 13 
    MID = 19 
    MID_INDEX_COUNT = 4
    LEFT_INDEX_COUNT = 6
    RIGHT_INDEX_COUNT = 6
    SOLO_INDEX_COUNT = 6
    LEN_ARR = 24


class BlockAccessFeatureMap:
    def __init__(
            self,
            block_size_byte: int = 4096
    ) -> None:
        self._block_count = 0 
        self._load_time_sec = -1
        self._map = {}
        self._block_size_byte = block_size_byte
        
        
    def keys(self) -> list:
        return self._map.keys()
    

    def get_current_block_count(self) -> int:
        return self._block_count
    
    
    def contains(self, block_addr: int) -> bool:
        return block_addr in self._map
    

    def get_total_request_count(
            self, 
            block_addr: int
    ) -> int:
        """Get the total request count of a block address in block access feature map.

        Args:
            block_addr: The block address whose request count is to be computed.
        
        Returns:
            total_req_count: The number of times, a block request is requested in the trace. It does not include the additional read request
                                made to blocks due to misalignment in write requests.
        """
        total_req_count = 0 
        if block_addr in self._map:
            block_stats = self._map[block_addr]
            total_req_count += (block_stats[MetadataIndex.SOLO.value] + block_stats[MetadataIndex.SOLO.value + 1])
            total_req_count += (block_stats[MetadataIndex.RIGHT.value] + block_stats[MetadataIndex.RIGHT.value + 1])
            total_req_count += (block_stats[MetadataIndex.LEFT.value] + block_stats[MetadataIndex.LEFT.value + 1])
            total_req_count += (block_stats[MetadataIndex.MID.value] + block_stats[MetadataIndex.MID.value + 1])
        return total_req_count
            
            
    def load(
            self,
            block_trace_path: Path
    ) -> None:
        """Load per block access stats given a block trace and the number of lower order bits of block
        addresses to ignore.

        Args:
            block_trace_path: Path to the block trace whose per-block access statistics will be loaded.
        """
        block_req_count = 0 
        start_time_ns = perf_counter_ns()
        assert not len(self._map.keys()), "Feature map should be empty when calling load."
        reader = CPReader(block_trace_path)
        cur_block_req = reader.get_next_block_req(block_size=self._block_size_byte)
        prev_ts = cur_block_req["ts"]
        while cur_block_req:
            block_req_count += 1
            start_block = cur_block_req["start_block"]
            end_block = cur_block_req["end_block"]
            cur_ts = cur_block_req["ts"]
            iat = cur_ts - prev_ts

            assert start_block <= end_block
            for cur_block in range(start_block, end_block+1):
                if cur_block not in self._map:
                    self._block_count += 1
                    self._map[cur_block] = zeros(MetadataIndex.LEN_ARR.value, dtype=int)

                sum_misalign_byte = 0 
                if start_block == end_block:
                    sum_misalign_byte = cur_block_req["front_misalign"] + cur_block_req["rear_misalign"]
                elif cur_block == start_block:
                    sum_misalign_byte = cur_block_req["front_misalign"]
                elif cur_block == end_block:
                    sum_misalign_byte = cur_block_req["rear_misalign"]
                
                if sum_misalign_byte > 0 and cur_block_req['op'] == 'w':
                    self._map[cur_block][0] += 1
                
                if start_block == end_block:
                    # solo 
                    metadata_index = MetadataIndex.SOLO.value
                else:
                    if cur_block == start_block:
                        # left 
                        metadata_index = MetadataIndex.LEFT.value
                    elif cur_block == end_block:
                        # right 
                        metadata_index = MetadataIndex.RIGHT.value
                    else:
                        # mid 
                        metadata_index = MetadataIndex.MID.value
                        
                if cur_block_req['op'] == 'r':
                    self._map[cur_block][metadata_index] += 1 
                    self._map[cur_block][metadata_index+2] += iat 
                    
                    try:
                        self._map[cur_block][metadata_index+4] += sum_misalign_byte
                    except IndexError:
                        assert metadata_index == 19
                else:
                    self._map[cur_block][metadata_index+1] += 1 
                    self._map[cur_block][metadata_index+3] += iat 

                    try:
                        self._map[cur_block][metadata_index+5] += sum_misalign_byte
                    except IndexError:
                        assert metadata_index == 19
                
            prev_ts = cur_ts 
            cur_block_req = reader.get_next_block_req(block_size=self._block_size_byte)
        
        reader.trace_file_handle.close()
        self._load_time_sec = (perf_counter_ns() - start_time_ns)/1e9
        
        
    def delete(
            self, 
            block_addr: int
    ) -> None:
        """Delete a block address from the feature map.

        Args:
            block_addr: The block address to be removed.
        """
        if block_addr not in self._map:
            raise ValueError("Block addr {} not in feature map of size {}.".format(block_addr, len(self._map.keys())))
        
        if block_addr - 1 in self._map:
            # all the request where block "block_addr - 1" was mid can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr -1" was mid now is the rightmost 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 6,7 and 8 were accessed together
            # where 7 used to be the middle block now have 7 as the right most block.
            self._map[block_addr-1][MetadataIndex.RIGHT.value] += self._map[block_addr-1][MetadataIndex.MID.value]
            self._map[block_addr-1][MetadataIndex.RIGHT.value + 2] += self._map[block_addr-1][MetadataIndex.MID.value + 2]

            self._map[block_addr-1][MetadataIndex.RIGHT.value + 1] += self._map[block_addr-1][MetadataIndex.MID.value + 1]
            self._map[block_addr-1][MetadataIndex.RIGHT.value + 3] += self._map[block_addr-1][MetadataIndex.MID.value + 3]

            # all the request where block "block_addr - 1" was left most can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr -1" was left most now is solo requests 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 7,8 and 9 were accessed together
            # where 7 used to be the left most block now is a separate block request where 7 is a solo block accessed.
            self._map[block_addr-1][MetadataIndex.SOLO.value] += self._map[block_addr-1][MetadataIndex.LEFT.value]
            self._map[block_addr-1][MetadataIndex.SOLO.value + 2] += self._map[block_addr-1][MetadataIndex.LEFT.value + 2]
            self._map[block_addr-1][MetadataIndex.SOLO.value + 4] += self._map[block_addr-1][MetadataIndex.LEFT.value + 4]

            self._map[block_addr-1][MetadataIndex.SOLO.value + 1] += self._map[block_addr-1][MetadataIndex.LEFT.value + 1]
            self._map[block_addr-1][MetadataIndex.SOLO.value + 3] += self._map[block_addr-1][MetadataIndex.LEFT.value + 3]
            self._map[block_addr-1][MetadataIndex.SOLO.value + 5] += self._map[block_addr-1][MetadataIndex.LEFT.value + 5]

            for i in range(MetadataIndex.LEFT_INDEX_COUNT.value):
                self._map[block_addr-1][MetadataIndex.LEFT.value + i] = 0
            
            for i in range(MetadataIndex.MID_INDEX_COUNT.value):
                self._map[block_addr-1][MetadataIndex.MID.value + i] = 0

        if block_addr + 1 in self._map:
            # all the request where block "block_addr + 1" was mid can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr + 1" was mid now is the leftmost 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 8,9 and 10 were accessed together
            # where 9 used to be the middle block now have 9 is the left most block.
            self._map[block_addr+1][MetadataIndex.LEFT.value] += self._map[block_addr+1][MetadataIndex.MID.value]
            self._map[block_addr+1][MetadataIndex.LEFT.value + 2] += self._map[block_addr+1][MetadataIndex.MID.value + 2]

            self._map[block_addr+1][MetadataIndex.LEFT.value + 1] += self._map[block_addr+1][MetadataIndex.MID.value + 1]
            self._map[block_addr+1][MetadataIndex.LEFT.value + 3] += self._map[block_addr+1][MetadataIndex.MID.value + 3]

            # all the request where block "block_addr + 1" was right most can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr + 1" was right most now is solo requests 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 7,8 and 9 were accessed together
            # where 9 used to be the right most block now is a separate block request where 9 is a solo block accessed.
            self._map[block_addr+1][MetadataIndex.SOLO.value] += self._map[block_addr+1][MetadataIndex.RIGHT.value]
            self._map[block_addr+1][MetadataIndex.SOLO.value + 2] += self._map[block_addr+1][MetadataIndex.RIGHT.value + 2]
            self._map[block_addr+1][MetadataIndex.SOLO.value + 4] += self._map[block_addr+1][MetadataIndex.RIGHT.value + 4]

            self._map[block_addr+1][MetadataIndex.SOLO.value + 1] += self._map[block_addr+1][MetadataIndex.RIGHT.value + 1]
            self._map[block_addr+1][MetadataIndex.SOLO.value + 3] += self._map[block_addr+1][MetadataIndex.RIGHT.value + 3]
            self._map[block_addr+1][MetadataIndex.SOLO.value + 5] += self._map[block_addr+1][MetadataIndex.RIGHT.value + 5]

            for i in range(MetadataIndex.RIGHT_INDEX_COUNT.value):
                self._map[block_addr+1][MetadataIndex.RIGHT.value + i] = 0
            
            for i in range(MetadataIndex.MID_INDEX_COUNT.value):
                self._map[block_addr+1][MetadataIndex.MID.value + i] = 0

        # delete the address 
        del self._map[block_addr]
        self._block_count -= 1
    

    def get_workload_feature_dict_on_removal(
            self, 
            workload_feature_dict: dict, 
            block_addr: int
    ) -> dict:
        """Given the current workload features from which the block access feature map was created and a block
        address, return the new workload features after the given block is removed from the trace.

        Args:
            workload_feature_dict: Dictionary of current workload features.
            block_addr: The block address to remove.
        
        Returns:
            new_workload_feature_dict: Dictionary of new workload features.
        """
        if block_addr not in self._map:
            raise ValueError("Block addr {} not in feature map of size {}.".format(block_addr, len(self._map.keys())))
        
        block_feature_arr = self._map[block_addr]

        read_iat_reduced = 0
        write_iat_reduced = 0 
        read_block_req_reduced = 0
        write_block_req_reduced = 0

        read_cache_req_reduced = 0 
        write_cache_req_reduced = 0 

        metadata_index = MetadataIndex.LEFT.value 
        read_block_req_byte_reduced = block_feature_arr[metadata_index] * self._block_size_byte
        write_block_req_byte_reduced = block_feature_arr[metadata_index + 1] * self._block_size_byte
        
        read_cache_req_reduced += block_feature_arr[metadata_index]
        write_cache_req_reduced += block_feature_arr[metadata_index + 1]

        read_block_misalignment_byte = block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte = block_feature_arr[metadata_index + 5]

        metadata_index = MetadataIndex.RIGHT.value 
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * self._block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * self._block_size_byte

        read_cache_req_reduced += block_feature_arr[metadata_index]
        write_cache_req_reduced += block_feature_arr[metadata_index + 1]

        read_block_misalignment_byte += block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte += block_feature_arr[metadata_index + 5]

        metadata_index = MetadataIndex.MID.value 
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * self._block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * self._block_size_byte

        read_cache_req_reduced += block_feature_arr[metadata_index]
        write_cache_req_reduced += block_feature_arr[metadata_index + 1]

        read_iat_reduced -= block_feature_arr[metadata_index + 2]
        write_iat_reduced -= block_feature_arr[metadata_index + 3]

        read_block_req_reduced -= block_feature_arr[metadata_index]
        write_block_req_reduced -= block_feature_arr[metadata_index + 1]

        metadata_index = MetadataIndex.SOLO.value
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * self._block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * self._block_size_byte

        read_cache_req_reduced += block_feature_arr[metadata_index]
        write_cache_req_reduced += block_feature_arr[metadata_index + 1]

        read_iat_reduced += block_feature_arr[metadata_index + 2]
        write_iat_reduced += block_feature_arr[metadata_index + 3]

        read_block_misalignment_byte += block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte += block_feature_arr[metadata_index + 5]

        read_block_req_reduced += block_feature_arr[metadata_index]
        write_block_req_reduced += block_feature_arr[metadata_index + 1]

        total_read_byte_reduced = read_block_req_byte_reduced - read_block_misalignment_byte
        total_write_byte_reduced = write_block_req_byte_reduced - write_block_misalignment_byte

        new_workload_feature_dict = deepcopy(workload_feature_dict)
        new_workload_feature_dict["read_block_req_count"] -= read_block_req_reduced
        new_workload_feature_dict["write_block_req_count"] -= write_block_req_reduced
        new_workload_feature_dict["read_block_req_byte"] -= total_read_byte_reduced
        new_workload_feature_dict["write_block_req_byte"] -= total_write_byte_reduced
        new_workload_feature_dict["iat_read_sum"] -= read_iat_reduced
        new_workload_feature_dict["iat_write_sum"] -= write_iat_reduced
        new_workload_feature_dict["read_cache_req_count"] -= (read_cache_req_reduced + block_feature_arr[MetadataIndex.MISALIGNMENT.value])
        new_workload_feature_dict["write_cache_req_count"] -= write_cache_req_reduced
        new_workload_feature_dict["read_misalignment_sum"] -= read_block_misalignment_byte
        new_workload_feature_dict["write_misalignment_sum"] -= write_block_misalignment_byte

        total_block_req_count = new_workload_feature_dict["write_block_req_count"] + new_workload_feature_dict["read_block_req_count"]
        total_cache_req_count = new_workload_feature_dict["write_cache_req_count"] + new_workload_feature_dict["read_cache_req_count"]
        write_block_req_ratio = new_workload_feature_dict["write_block_req_count"]/total_block_req_count
        write_cache_req_ratio = new_workload_feature_dict["write_cache_req_count"]/total_cache_req_count

        new_workload_feature_dict["write_block_req_split"] = write_block_req_ratio
        new_workload_feature_dict["write_cache_req_split"] = write_cache_req_ratio

        new_workload_feature_dict["iat_read_avg"] = new_workload_feature_dict["iat_read_sum"]/new_workload_feature_dict["read_block_req_count"] \
                                                        if new_workload_feature_dict["read_block_req_count"] > 0 else 0.0
        new_workload_feature_dict["iat_write_avg"] = new_workload_feature_dict["iat_write_sum"]/new_workload_feature_dict["write_block_req_count"] \
                                                        if new_workload_feature_dict["write_block_req_count"] > 0 else 0.0
        
        new_workload_feature_dict["read_size_avg"] = new_workload_feature_dict["read_block_req_byte"]/new_workload_feature_dict["read_block_req_count"] \
                                                        if new_workload_feature_dict["read_block_req_count"] > 0 else 0.0
        new_workload_feature_dict["write_size_avg"] = new_workload_feature_dict["write_block_req_byte"]/new_workload_feature_dict["write_block_req_count"] \
                                                        if new_workload_feature_dict["write_block_req_count"] > 0 else 0.0
        
        new_workload_feature_dict["read_misalignment_per_req"] = new_workload_feature_dict["read_misalignment_sum"]/new_workload_feature_dict["read_block_req_count"] \
                                                                    if new_workload_feature_dict["read_block_req_count"] > 0 else 0.0
        new_workload_feature_dict["write_misalignment_per_req"] = new_workload_feature_dict["write_misalignment_sum"]/new_workload_feature_dict["write_block_req_count"] \
                                                                    if new_workload_feature_dict["write_block_req_count"] > 0 else 0.0
        
        return new_workload_feature_dict