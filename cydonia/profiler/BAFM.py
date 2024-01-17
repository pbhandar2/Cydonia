from enum import Enum
from copy import deepcopy
from pathlib import Path
from time import perf_counter_ns
from dataclasses import dataclass
from json import loads, dumps
from pandas import DataFrame, read_csv, concat, set_option
from numpy import zeros, ndarray, mean, array, multiply
from time import perf_counter_ns

from cydonia.profiler.CacheTrace import CacheTraceReader, ReaderConfig
from cydonia.profiler.WorkloadStats import WorkloadStats, BlockStats, MisalignStats, BlockRequest, NpEncoder


class MetadataIndex(Enum):
    RMISALIGNMENT_I = 0 
    WMISALIGNMENT_I = 1
    SOLO_I = 2
    LEFT_I = 8
    RIGHT_I = 14
    MID_I = 20
    MID_LEN= 4
    LEFT_LEN = 6
    RIGHT_LEN = 6
    SOLO_LEN = 6
    LEN = 24


@dataclass
class Record:
    addr: int 
    write_flag: bool 
    misalign_count: int 
    misalign_byte: int 
    iat: int 
    index: int 


ACCESS_FILE_HEADER = ["r_misalign", "w_misalign", 
                        "solo_r", "solo_w", "solo_r_iat", "solo_w_iat", 
                        "solo_r_misalign_byte", "solo_w_misalign_byte",
                        "left_r", "left_w", "left_r_iat", "left_w_iat", 
                        "left_r_misalign_byte", "left_w_misalign_byte",
                        "right_r", "right_w", "right_r_iat", "right_w_iat", 
                        "right_r_misalign_byte", "right_w_misalign_byte",
                        "mid_r", "mid_w", "mid_r_iat", "mid_w_iat"]


class BAFMOutput:
    def __init__(self, path: Path):
        self._path = path 
        self._header = list(WorkloadStats().get_workload_feature_dict().keys()).extend(["mean", "max", "wmean"])
        self._df = read_csv(self._path, names=self._header, float_precision='round_trip') if self._path.exists() else None 
    

    def add(self, err_dict: dict):
        if self._df is None:
            self._df = DataFrame([err_dict])
        else:
            assert err_dict["addr"] not in self._df, "Address {} already exists.".format(err_dict["addr"])
            self._df = concat([self._df, DataFrame([err_dict])], ignore_index=True)
        self._df.to_csv(self._path, index=False)


    def get_addr_removed(self):
        return self._df["addr"].to_list()


    def get_last_err_dict(self):
        return loads(self._df.iloc[-1].to_json())
    

    def num_blocks_removed(self) -> int:
        return len(self._df)
    

class BAFM:
    def __init__(
            self, 
            lower_addr_bits_ignored: int 
    ) -> None:
        # the map with address as key and array of access features as values 
        self._map = {}
        # the number of blocks in the map 
        self._block_count = 0 
        self._lower_addr_bits_ignored = lower_addr_bits_ignored
        self._workload_stat = None 


    def delete(
            self, 
            block_addr: int
    ) -> None:
        """ Delete a block address from the feature map and make the necessary adjustments.

        Args:
            block_addr: The block address to be removed.
        
        Raises:
            ValueError: If the block address to be deleted is not in the map.
        """
        if block_addr not in self._map:
            raise ValueError("Block addr {} not in feature map of size {}.".format(block_addr, len(self._map.keys())))
        
        if block_addr - 1 in self._map:
            # all the request where block "block_addr - 1" was mid can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr -1" was mid now is the rightmost 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 6,7 and 8 were accessed together
            # where 7 used to be the middle block now have 7 as the right most block.
            self._map[block_addr-1][MetadataIndex.RIGHT_I.value] += self._map[block_addr-1][MetadataIndex.MID_I.value]
            self._map[block_addr-1][MetadataIndex.RIGHT_I.value + 2] += self._map[block_addr-1][MetadataIndex.MID_I.value + 2]

            self._map[block_addr-1][MetadataIndex.RIGHT_I.value + 1] += self._map[block_addr-1][MetadataIndex.MID_I.value + 1]
            self._map[block_addr-1][MetadataIndex.RIGHT_I.value + 3] += self._map[block_addr-1][MetadataIndex.MID_I.value + 3]

            # all the request where block "block_addr - 1" was left most can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr -1" was left most now is solo requests 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 7,8 and 9 were accessed together
            # where 7 used to be the left most block now is a separate block request where 7 is a solo block accessed.
            self._map[block_addr-1][MetadataIndex.SOLO_I.value] += self._map[block_addr-1][MetadataIndex.LEFT_I.value]
            self._map[block_addr-1][MetadataIndex.SOLO_I.value + 2] += self._map[block_addr-1][MetadataIndex.LEFT_I.value + 2]
            self._map[block_addr-1][MetadataIndex.SOLO_I.value + 4] += self._map[block_addr-1][MetadataIndex.LEFT_I.value + 4]

            self._map[block_addr-1][MetadataIndex.SOLO_I.value + 1] += self._map[block_addr-1][MetadataIndex.LEFT_I.value + 1]
            self._map[block_addr-1][MetadataIndex.SOLO_I.value + 3] += self._map[block_addr-1][MetadataIndex.LEFT_I.value + 3]
            self._map[block_addr-1][MetadataIndex.SOLO_I.value + 5] += self._map[block_addr-1][MetadataIndex.LEFT_I.value + 5]

            for i in range(MetadataIndex.LEFT_LEN.value):
                self._map[block_addr-1][MetadataIndex.LEFT_I.value + i] = 0
            
            for i in range(MetadataIndex.MID_LEN.value):
                self._map[block_addr-1][MetadataIndex.MID_I.value + i] = 0

        if block_addr + 1 in self._map:
            # all the request where block "block_addr + 1" was mid can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr + 1" was mid now is the leftmost 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 8,9 and 10 were accessed together
            # where 9 used to be the middle block now have 9 is the left most block.
            self._map[block_addr+1][MetadataIndex.LEFT_I.value] += self._map[block_addr+1][MetadataIndex.MID_I.value]
            self._map[block_addr+1][MetadataIndex.LEFT_I.value + 2] += self._map[block_addr+1][MetadataIndex.MID_I.value + 2]

            self._map[block_addr+1][MetadataIndex.LEFT_I.value + 1] += self._map[block_addr+1][MetadataIndex.MID_I.value + 1]
            self._map[block_addr+1][MetadataIndex.LEFT_I.value + 3] += self._map[block_addr+1][MetadataIndex.MID_I.value + 3]

            # all the request where block "block_addr + 1" was right most can no longer exist since block_addr is no longer in the sample
            # so all request count and IAT sum of when "block_addr + 1" was right most now is solo requests 
            # for instance, Block 5,6,7,8,9,10 are sampled. We removed 8 now all requests where 7,8 and 9 were accessed together
            # where 9 used to be the right most block now is a separate block request where 9 is a solo block accessed.
            self._map[block_addr+1][MetadataIndex.SOLO_I.value] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value]
            self._map[block_addr+1][MetadataIndex.SOLO_I.value + 2] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value + 2]
            self._map[block_addr+1][MetadataIndex.SOLO_I.value + 4] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value + 4]

            self._map[block_addr+1][MetadataIndex.SOLO_I.value + 1] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value + 1]
            self._map[block_addr+1][MetadataIndex.SOLO_I.value + 3] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value + 3]
            self._map[block_addr+1][MetadataIndex.SOLO_I.value + 5] += self._map[block_addr+1][MetadataIndex.RIGHT_I.value + 5]

            for i in range(MetadataIndex.RIGHT_LEN.value):
                self._map[block_addr+1][MetadataIndex.RIGHT_I.value + i] = 0
            
            for i in range(MetadataIndex.MID_LEN.value):
                self._map[block_addr+1][MetadataIndex.MID_I.value + i] = 0

        # delete the address 
        del self._map[block_addr]
        self._block_count -= 1

    
    def update(self, record: Record) -> None:
        """ Update the map with a given record.

        Args:
            record: Record object to be updated. 
        """
        if record.addr not in self._map:
            # new address found! 
            self._map[record.addr] = zeros(MetadataIndex.LEN.value, dtype=int)
            self._block_count += 1
        
        if record.write_flag:
            self._map[record.addr][MetadataIndex.WMISALIGNMENT_I.value] += record.misalign_count
            self._map[record.addr][record.index+1] += 1 
            self._map[record.addr][record.index+3] += record.iat

            """ The middle block of a multi-block request is accessed in its entirety and cannot
            have any misalignment. For rest of the request, index+4 and index+5 hold the misalignment
            data. Instead of checking everytime if the index is equal to the index of MetadataIndex.MID_I
            and avoiding updating misalignment, I choose to handle the IndexError which will occur
            only when updating index for MID requests so that the values are not updated and we make
            sure that the index was MetadataIndex.MID_I. I think this reduces the number of comparison
            and work to be done. """
            try:
                self._map[record.addr][record.index+5] += record.misalign_byte
            except IndexError:
                assert record.index == MetadataIndex.MID_I.value

        else:
            self._map[record.addr][MetadataIndex.RMISALIGNMENT_I.value] += record.misalign_count
            self._map[record.addr][record.index] += 1 
            self._map[record.addr][record.index+2] += record.iat

            # Same comment as in the "if" block above.
            try:
                self._map[record.addr][record.index+4] += record.misalign_byte
            except IndexError:
                assert record.index == MetadataIndex.MID_I.value


    def load_block_access_file(self, access_file_path: Path) -> None:
        """ Load data from access features file to this BAFM. 

        Args:
            access_file_path: Path of the access features file. 
            ignore_addr_set: Set of addresses to remove from the AccessFeatureMap.
        """
        df = read_csv(access_file_path)
        for _, row in df.iterrows():
            self._map[row["addr"]] = zeros(MetadataIndex.LEN.value, dtype=int)
            for index, header in enumerate(ACCESS_FILE_HEADER):
                self._map[row["addr"]][index] = row[header]
        self._block_count = len(df)

    
    def update_state(
            self, 
            output_file_path: Path, 
            sample_workload_stats: WorkloadStats
    ) -> WorkloadStats:
        """ Update the algorithm to some state. 
        
        Args:
            output_file_path: Path to output file to load the state from.  
            sample_workload_stats: Workload stats at the begining of computation. 
        
        Return:
            workload_stats: WorkloadStats 
        """
        bafm_output_file = BAFMOutput(output_file_path)
        ignore_addr_list = bafm_output_file.get_addr_removed()
        cur_workload_stats = deepcopy(sample_workload_stats)

        # remove the blocks that should be ignored 
        for ignore_addr in ignore_addr_list:
            cur_workload_stats = self.get_new_workload_stat(cur_workload_stats, self._map[ignore_addr])
            self.delete(ignore_addr)
        
        cur_err_dict = self.get_error_dict(sample_workload_stats.get_workload_feature_dict(), 
                                            cur_workload_stats.get_workload_feature_dict())
        last_err_dict = bafm_output_file.get_last_err_dict()

        #assert cur_err_dict == last_err_dict, "The dicts are not the same."
        for feature_name in cur_err_dict.keys():
            cur_val = cur_err_dict[feature_name]
            last_val = last_err_dict[feature_name]
            assert abs(cur_val - last_val) < 1e6, "Feature {} did not match {} vs {}.".format(feature_name, cur_val, last_val)
        
        return cur_workload_stats


    def load_cache_trace(
            self, 
            sample_cache_trace_path: Path
    ) -> None:
        """ Load cache trace. 

        Args:
            sample_cache_trace_path: The Path of cache trace.
        """
        cur_ts = -1 
        start_time = perf_counter_ns()
        reader = CacheTraceReader(sample_cache_trace_path)
        cache_req_df = reader.get_next_cache_req_group_df()
        allocation_size_byte = self.get_block_size_from_lower_bits_ignored(self._lower_addr_bits_ignored, reader._config.cache_block_size_byte)
        while len(cache_req_df):
            blk_req_arr = reader.get_block_req_arr(cache_req_df, cur_ts, reader._config)
            if cur_ts == -1:
                # first request, make sure IAT is 0 
                cur_ts = blk_req_arr[0].ts
            
            for blk_req in blk_req_arr:
                record_arr = self.get_request_arr(cur_ts, blk_req, allocation_size_byte, reader._config)
                for record in record_arr:
                    self.update(record)
                cur_ts = blk_req.ts
            cache_req_df = reader.get_next_cache_req_group_df()
        reader.close()
        print("Cache trace {} loaded in {} minutes.".format(sample_cache_trace_path, (perf_counter_ns()-start_time)/(1e9*60)))


    def write_map_to_file(
            self, 
            output_file_path: Path
    ) -> None:
        """ Write map to file.

        Args:
            output_file_path: The output path of feature map. 
        """
        df = DataFrame.from_dict(self._map, orient='index', columns=ACCESS_FILE_HEADER)
        df.index.name = 'addr'
        df.to_csv(output_file_path)


    def target_sampling_rate(
            self,
            full_workload_stat: WorkloadStats,
            sample_workload_stat: WorkloadStats,
            metric_name: str,
            full_workload_block_count: int,
            sample_block_addr_set: set, 
            target_sampling_rate: float, 
            output_file_path: Path,
            print_interval_sec: int = 60
    ) -> WorkloadStats:
        """ Remove blocks until we hit a target sampling rate.

        Args:
            full_workload_stat: Workload stats of the full workload.
            sample_workload_stat: Workload stats of the sample workload.
            metric_name: The metric to use when selecting blocks.
            full_workload_block_count: The number of unique blocks in full trace.
            target_sampling_rate: The target sampling rate.
            output_file_path: Path of the output file.
            print_interval_sec: The interval at which latest error dictionary is printed.
        
        Returns:
            new_workload_stat: New workload stat after removing blocks.
        """
        print_interval_tracker = 0 
        bafm_output = BAFMOutput(output_file_path)
        new_workload_stat = deepcopy(sample_workload_stat)
        cur_sampling_rate = len(sample_block_addr_set)/full_workload_block_count
        print("Starting sampling rate is {} and target sampling rate is {}.".format(cur_sampling_rate, target_sampling_rate))
        while cur_sampling_rate > target_sampling_rate:
            start_time_ns = perf_counter_ns()
            best_dict = self.find_best_block_to_remove(full_workload_stat, new_workload_stat, metric_name)
            if not best_dict:
                print("Ran out of blocks to remove.")
                break 

            feature_arr = self._map[best_dict["addr"]]
            new_workload_stat = self.get_new_workload_stat(new_workload_stat, feature_arr)
            err_dict = self.get_error_dict(full_workload_stat.get_workload_feature_dict(),
                                            new_workload_stat.get_workload_feature_dict())
            
            err_dict["addr"] = best_dict["addr"]

            # remove the sclaed address from this BAFM
            self.delete(best_dict["addr"])

            # remove unscaled block addresses from sample block address set 
            unscaled_addr_list = CacheTraceReader.get_blk_addr_arr(best_dict["addr"], self._lower_addr_bits_ignored)
            for cur_addr in unscaled_addr_list:
                if cur_addr in sample_block_addr_set:
                    sample_block_addr_set.remove(cur_addr)
                
            cur_sampling_rate = len(sample_block_addr_set)/full_workload_block_count
            err_dict["block_count"] = len(sample_block_addr_set)
            err_dict["rate"] = cur_sampling_rate
            err_dict["runtime"] = perf_counter_ns() - start_time_ns
            bafm_output.add(err_dict)

            # print latest error dictionary at regular intervals 
            print_interval_tracker += err_dict["runtime"]
            if (print_interval_tracker/1e9) > print_interval_sec:
                print(err_dict)
                print_interval_tracker = 0 
            

    def remove_n_blocks(
            self,
            full_workload_stat: WorkloadStats,
            sample_workload_stat: WorkloadStats,
            metric_name: str,
            num_iter: int,
            output_file_path: Path 
    ) -> WorkloadStats:
        """ Remove "N" blocks from the workload.

        Args:
            workload_stat: Workload stats when starting to remove blocks.
            metric_name: The metric to use when selecting blocks.
            num_iter: Number of blocks to remove. 
            output_file_path: Path of the output file.
        
        Returns:
            new_workload_stat: New workload stat after removing blocks.
        """
        bafm_output = BAFMOutput(output_file_path)
        new_workload_stat = deepcopy(sample_workload_stat)

        for _ in range(num_iter):
            best_dict = self.find_best_block_to_remove(full_workload_stat, new_workload_stat, metric_name)
            if not best_dict:
                break 

            feature_arr = self._map[best_dict["addr"]]
            new_workload_stat = self.get_new_workload_stat(new_workload_stat, feature_arr)
            err_dict = self.get_error_dict(full_workload_stat.get_workload_feature_dict(),
                                            new_workload_stat.get_workload_feature_dict())
            err_dict["addr"] = best_dict["addr"]
            
            bafm_output.add(err_dict)
            self.delete(best_dict["addr"])
        return new_workload_stat


    
    def find_best_block_to_remove(
            self, 
            full_workload_stat: WorkloadStats,
            sample_workload_stat: WorkloadStats, 
            metric_name: str
    ) -> dict:
        """ Find the best block to remove. 

        Args:
            start_workload_feature: Feature of workload to remove blocks from.  
            metric_name: Name of the metric to optimize.  
        
        Returns:
            best_dict: Dictionary of error values. 
        """

        best_err_dict = {}
        full_workload_feature_dict = full_workload_stat.get_workload_feature_dict()
        for addr in self._map:
            # get the new sample workload stats after removing an address 
            new_sample_workload_stat = self.get_new_workload_stat(sample_workload_stat, self._map[addr])

            # compute the error value if we remove this address 
            new_sample_workload_feature_dict = new_sample_workload_stat.get_workload_feature_dict()
            err_dict = self.get_error_dict(full_workload_feature_dict, new_sample_workload_feature_dict)
            err_dict["addr"] = addr 

            if not best_err_dict:
                best_err_dict = err_dict 
            else:
                if best_err_dict[metric_name] > err_dict[metric_name]:
                    best_err_dict = err_dict

        return best_err_dict


    def get_new_workload_stat(
            self,
            workload_stat: WorkloadStats, 
            block_feature_arr: ndarray
    ) -> WorkloadStats:
        """ Get new workload stats when a block with the given features is removed from a workload
        with the given features. 

        Args:
            workload_stat: WorkloadStats object representing the workload features.
            block_feature_arr: Array of features of the block to be removed. 
        """
        block_size_byte = self.get_block_size_from_lower_bits_ignored(self._lower_addr_bits_ignored, 4096)

        read_iat_reduced = 0
        write_iat_reduced = 0 
        read_block_req_reduced = 0
        write_block_req_reduced = 0

        metadata_index = MetadataIndex.LEFT_I.value 
        read_block_req_byte_reduced = block_feature_arr[metadata_index] * block_size_byte
        write_block_req_byte_reduced = block_feature_arr[metadata_index + 1] * block_size_byte

        read_block_misalignment_byte = block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte = block_feature_arr[metadata_index + 5]

        metadata_index = MetadataIndex.RIGHT_I.value 
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * block_size_byte

        read_block_misalignment_byte += block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte += block_feature_arr[metadata_index + 5]

        metadata_index = MetadataIndex.MID_I.value
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * block_size_byte

        read_iat_reduced -= block_feature_arr[metadata_index + 2]
        write_iat_reduced -= block_feature_arr[metadata_index + 3]

        read_block_req_reduced -= block_feature_arr[metadata_index]
        write_block_req_reduced -= block_feature_arr[metadata_index + 1]

        metadata_index = MetadataIndex.SOLO_I.value 
        read_block_req_byte_reduced += block_feature_arr[metadata_index] * block_size_byte
        write_block_req_byte_reduced += block_feature_arr[metadata_index + 1] * block_size_byte

        read_iat_reduced += block_feature_arr[metadata_index + 2]
        write_iat_reduced += block_feature_arr[metadata_index + 3]

        read_block_misalignment_byte += block_feature_arr[metadata_index + 4]
        write_block_misalignment_byte += block_feature_arr[metadata_index + 5]

        read_block_req_reduced += block_feature_arr[metadata_index]
        write_block_req_reduced += block_feature_arr[metadata_index + 1]

        metadata_index = MetadataIndex.RMISALIGNMENT_I.value 
        read_misalignment_reduced = block_feature_arr[metadata_index]

        metadata_index = MetadataIndex.WMISALIGNMENT_I.value 
        write_misalignment_reduced = block_feature_arr[metadata_index]

        total_read_byte_reduced = read_block_req_byte_reduced - read_block_misalignment_byte
        total_write_byte_reduced = write_block_req_byte_reduced - write_block_misalignment_byte

        new_workload_stat = {}
        new_workload_stat["block_read_count"] = workload_stat._block_stat.block_read_count - read_block_req_reduced
        new_workload_stat["block_write_count"] = workload_stat._block_stat.block_write_count - write_block_req_reduced
        new_workload_stat["block_read_iat_sum"] = workload_stat._block_stat.block_read_iat_sum - read_iat_reduced
        new_workload_stat["block_write_iat_sum"] = workload_stat._block_stat.block_write_iat_sum - write_iat_reduced
        new_workload_stat["block_read_byte_sum"] = workload_stat._block_stat.block_read_byte_sum - total_read_byte_reduced
        new_workload_stat["block_write_byte_sum"] = workload_stat._block_stat.block_write_byte_sum - total_write_byte_reduced

        new_workload_stat["misaligned_read_count"] = workload_stat._misalign_stat.misaligned_read_count - read_misalignment_reduced
        new_workload_stat["misaligned_write_count"] = workload_stat._misalign_stat.misaligned_write_count - write_misalignment_reduced

        new_workload_stat["misaligned_read_byte"] = workload_stat._misalign_stat.misaligned_read_byte - read_block_misalignment_byte
        new_workload_stat["misaligned_write_byte"] = workload_stat._misalign_stat.misaligned_write_byte - write_block_misalignment_byte

        new_workload_stat["misaligned_read_cache_req_count"] = 0
        new_workload_stat["misaligned_write_cache_req_count"] = 0

        new_stat = WorkloadStats()
        new_stat.load_dict(new_workload_stat)
        return new_stat
    

    @staticmethod
    def get_request_arr(
            prev_ts: int, 
            blk_req: BlockRequest, 
            allocation_size_byte: int, 
            config: ReaderConfig
    ) -> None:
        start_offset = blk_req.lba * config.lba_size_byte
        end_offset = start_offset + blk_req.size_byte
        iat = blk_req.ts - prev_ts 
        write_flag = blk_req.write_flag
        assert prev_ts <= blk_req.ts

        front_misalign_byte, rear_misalign_byte = start_offset % allocation_size_byte, \
                                                    allocation_size_byte - (end_offset % allocation_size_byte)
        
        front_misalign_count, rear_misalign_count = 0, 0 
        if front_misalign_byte > 0:
            front_misalign_count = 1 if front_misalign_byte % config.cache_block_size_byte > 0 else 0
        if rear_misalign_byte > 0:
            rear_misalign_count = 1 if rear_misalign_byte % config.cache_block_size_byte > 0 else 0
        
        start_addr = start_offset//allocation_size_byte
        end_addr = (end_offset - 1)//allocation_size_byte

        req_arr = []
        for addr in range(start_addr, end_addr + 1):
            if start_addr == end_addr:
                req_arr.append(Record(addr, 
                                        write_flag, 
                                        front_misalign_count + rear_misalign_count, 
                                        front_misalign_byte + rear_misalign_byte,
                                        iat,
                                        MetadataIndex.SOLO_I.value))
            else:
                if addr == start_addr:
                    req_arr.append(Record(addr, 
                                            write_flag, 
                                            front_misalign_count,
                                            front_misalign_byte,
                                            iat, 
                                            MetadataIndex.LEFT_I.value))
                elif addr == end_addr:
                    req_arr.append(Record(addr, 
                                            write_flag, 
                                            rear_misalign_count,
                                            rear_misalign_byte,
                                            iat, 
                                            MetadataIndex.RIGHT_I.value))
                else:
                    req_arr.append(Record(addr, 
                                            write_flag, 
                                            0,
                                            0,
                                            iat, 
                                            MetadataIndex.MID_I.value))
        
        return req_arr


    @staticmethod
    def get_block_size_from_lower_bits_ignored(lower_bits_ignored: int, cache_block_size_byte: int):
        return (2**lower_bits_ignored) * cache_block_size_byte
    

    @staticmethod
    def get_error_dict(
            w1_feature_dict, 
            w2_feature_dict
    ) -> dict:
        """ Compute dictionary of error metrics. 

        Args:
            w1_feature_dict: Dictionary of workload features of workload 1.
            w2_feature_dict: Dictionary of workload features of workload 2.
        
        Returns:
            err_dict: Dictionary of error metrics. 
        """
        err_arr = []
        err_dict = {}
        for feature_key in w1_feature_dict:
            err_val = 100*(w1_feature_dict[feature_key] - w2_feature_dict[feature_key])/w1_feature_dict[feature_key]
            err_arr.append(err_val)
            err_dict[feature_key] = err_val
        abs_err_arr = array([abs(_) for _ in err_arr], dtype=float)
        err_dict["mean"] = mean(abs_err_arr)
        err_dict["max"] = max(abs_err_arr)
        weight_arr = abs_err_arr/sum(abs_err_arr) if sum(abs_err_arr) > 0 else zeros(len(abs_err_arr))
        err_dict["wmean"] = sum(multiply(abs_err_arr, weight_arr))
        return err_dict


    def __eq__(self, other) -> bool:
        if self._block_count != other._block_count:
            return False 
        
        for other_blk_addr in other._map:
            if other_blk_addr not in self._map:
                return False 

            if not all(other._map[other_blk_addr] == self._map[other_blk_addr]):
                return False 
        return True 