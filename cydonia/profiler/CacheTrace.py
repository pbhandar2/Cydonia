""" CacheTrace loads a cache trace generated from a block trace using BlockTraceReader.

Usage:
    cache_trace = CacheTrace(cache_trace_path)
"""
from copy import deepcopy
from enum import Enum
from typing import List
from pathlib import Path 
from pandas import read_csv, DataFrame
from numpy import ndarray, zeros 
from time import perf_counter_ns
from queue import PriorityQueue
import mmh3

from cydonia.profiler.BlockTrace import ReaderConfig

from cydonia.profiler.WorkloadStats import WorkloadStats, BlockRequest


class HASH_FILE_CONFIG(Enum):
    ADDR_HEADER_NAME = "addr"
    HASH_HEADER_NAME = "hash"
    HASH_FILE_HEADER_NAME_ARR = ["hash", "addr"]


class HashFile:
    def __init__(self, file_path: Path):
        self._path = file_path 
        self._df = None 

    def get_hash_priority_queue(
            self,
            unique_addr_set: set, 
            random_seed: int 
    ) -> PriorityQueue:
        queue = PriorityQueue()
        for addr in unique_addr_set:
            queue.put((mmh3.hash128(str(addr), signed=False, seed=random_seed), addr))
        return queue 
    
    def load(self):
        header_name_arr = HASH_FILE_CONFIG.HASH_FILE_HEADER_NAME_ARR.value
        df = read_csv(self._path, names=header_name_arr)
        self._df = df.sort_values(by=[HASH_FILE_CONFIG.HASH_HEADER_NAME.value])
    
    def create(self, unique_addr_set: set, random_seed: int):
        hash_queue = self.get_hash_priority_queue(unique_addr_set, random_seed)
        with self._path.open("w+") as output_file_handle:
            while not hash_queue.empty():
                hash, addr = hash_queue.get()
                output_file_handle.write("{},{}\n".format(hash, addr))


class CacheTraceReader:
    def __init__(
            self,
            file_path: Path, 
            config: ReaderConfig = ReaderConfig()
    ) -> None:
        self._config = config 
        self._header = config.get_cache_trace_header()
        self._index_map = {
            self._config.iat_header_name: self._header.index(self._config.iat_header_name),
            self._config.req_index_header_name: self._header.index(self._config.req_index_header_name),
            self._config.op_header_name: self._header.index(self._config.op_header_name),
            self._config.cache_addr_header_name: self._header.index(self._config.cache_addr_header_name),
            self._config.front_misalign_header_name: self._header.index(self._config.front_misalign_header_name),
            self._config.rear_misalign_header_name: self._header.index(self._config.rear_misalign_header_name)
        }
        self._path = file_path 
        self._handle = file_path.open("r")
        self._prev_cache_req = {}
        self._first_req = {}
    

    def get_stat(self, print_every_n: int = 1e6):
        self.reset()
        start_time = perf_counter_ns()
        cur_ts = 0 
        block_req_processed = 0 
        workload_stats = WorkloadStats()
        cache_req_group_df = self.get_next_cache_req_group_df()
        while len(cache_req_group_df):
            block_req_arr = CacheTraceReader.get_block_req_arr(cache_req_group_df, cur_ts, self._config)
            for block_req in block_req_arr:
                workload_stats.track(block_req)
                cur_ts = block_req.ts
                block_req_processed += 1 
                if block_req_processed % print_every_n == 0:
                    print("{} requests processed in {} minutes.".format(block_req_processed, 
                                                                            (perf_counter_ns()-start_time)/(1e9*60)))
            cache_req_group_df = self.get_next_cache_req_group_df()
        return workload_stats
    

    def get_next_cache_req(self):
        """ Get next cache request from the cache trace. """
        cache_req = {}
        trace_line = self._handle.readline().rstrip()
        if trace_line:
            split_trace_line = trace_line.split(self._config.delimiter)
            cache_req[self._config.iat_header_name] = int(split_trace_line[self._index_map[self._config.iat_header_name]])
            cache_req[self._config.cache_addr_header_name] = int(split_trace_line[self._index_map[self._config.cache_addr_header_name]])
            cache_req[self._config.op_header_name] = split_trace_line[self._index_map[self._config.op_header_name]]
            cache_req[self._config.req_index_header_name] = int(split_trace_line[self._index_map[self._config.req_index_header_name]])
            cache_req[self._config.front_misalign_header_name] = int(split_trace_line[self._index_map[self._config.front_misalign_header_name]])
            cache_req[self._config.rear_misalign_header_name] = int(split_trace_line[self._index_map[self._config.rear_misalign_header_name]])
        return cache_req 
    

    def get_next_cache_req_group_df(self):
        """ Get a DataFrame of next cache requests originating from the same block request. """
        cache_req_arr = []

        next_cache_req = self.get_next_cache_req()
        if not next_cache_req and not self._prev_cache_req:
            # if there is no previous or next request then we have run out of requests!
            return cache_req_arr

        if not self._prev_cache_req:
            # first cache request 
            self._prev_cache_req = deepcopy(next_cache_req)
        else:
            cache_req_arr.append(self._prev_cache_req)
            
        while next_cache_req and \
                (next_cache_req[self._config.req_index_header_name] == self._prev_cache_req[self._config.req_index_header_name]):
            cache_req_arr.append(next_cache_req)
            next_cache_req = self.get_next_cache_req() 

        self._prev_cache_req = deepcopy(next_cache_req)
        return DataFrame(cache_req_arr)
    

    def get_unique_block_addr_set(self, num_lower_addr_bits_ignored: int) -> set:
        """ Get a set of unique block addresses. 
        
        Args:
            num_lower_addr_bits_ignored: Number of lower order address bits ignored.
        
        Returns:
            block_addr_set: Set of block addresses in the cache trace. 
        """
        self.reset()
        cur_cache_req = self.get_next_cache_req()
        unique_block_set = set()
        while cur_cache_req:
            cur_addr = cur_cache_req[self._config.cache_addr_header_name] >> num_lower_addr_bits_ignored
            unique_block_set.add(cur_addr)
            cur_cache_req = self.get_next_cache_req()
        return unique_block_set
    

    def get_unscaled_unique_block_addr_set(self) -> set:
        """ Get unique block addresses set where no bits are ignored. """
        return self.get_unique_block_addr_set(0)

    
    def create_sample_hash_file(
            self,
            random_seed: int, 
            num_lower_addr_bits_ignored: int,
            sample_hash_file_path: Path
    ) -> None:
        """ Create a new hash file from the cache trace.
        
        Args:
            random_seed: Random seed. 
            num_lower_addr_bits_ignored: Number of lower order address bits ignored.
            sample_hash_file_path: Path of the hash file to be created. 
        """
        unique_block_addr_set = self.get_unique_block_addr_set(num_lower_addr_bits_ignored)
        CacheTraceReader.create_sample_hash_file_for_addr_set(unique_block_addr_set, random_seed, sample_hash_file_path)
    

    def sample_using_hash_file(
            self,
            hash_file_path: int,
            rate: float,
            num_lower_addr_bits_ignored: int,
            sample_file_path: Path 
    ) -> None:
        """ Sample this cache trace using a hash file. 

        Args:
            hash_file_path: Hash file to use for sampling.
            rate: Rate of sampling. 
            num_lower_addr_bits_ignored: Number of lower order address bits ignored.
            sample_file_path: Path of the sample.
        """
        assert rate > 0.0 and rate < 1.0 
        unscaled_unique_addr_set = self.get_unique_block_addr_set(0)
        hash_file = HashFile(hash_file_path)
        hash_file.load()
        print("Ready to sample requests of file {} with rate {}.".format(self._path, rate))
        sample_addr_dict = self.get_sample_addr_dict(hash_file, unscaled_unique_addr_set, num_lower_addr_bits_ignored, rate)
        self.sample(sample_addr_dict, sample_file_path)

    
    def get_sample_addr_dict(
            self, 
            hash_file: HashFile, 
            unscaled_unique_addr_set: dict, 
            num_lower_addr_bits_ignored: int, 
            rate: float
    ) -> dict:
        """ Get a dictionary with block addresses to be sampled as keys.
        
        Args:
            hash_file: The HashFile object with the hash file loaded.
            unscaled_unique_addr_set: Dictionary of block addresses in the original cache trace.
            num_lower_addr_bits_ignored: Number of lower order address bits ignored.
            rate: Rate of sampling.
        
        Returns:
            sample_addr_dict: Dictionary with sampled block addresses as keys. 
        """
        num_unscaled_unique_addr_count = len(unscaled_unique_addr_set)
        sample_count = 0 
        sample_addr_dict = {}
        for _, sample_hash_row in hash_file._df.iterrows():
            if sample_count/num_unscaled_unique_addr_count >= rate:
                break

            blk_addr_arr = self.get_blk_addr_arr(sample_hash_row[HASH_FILE_CONFIG.ADDR_HEADER_NAME.value], num_lower_addr_bits_ignored)
            for blk_addr in blk_addr_arr:
                if blk_addr in unscaled_unique_addr_set:
                    sample_addr_dict[blk_addr] = True 
                    sample_count += 1
        return sample_addr_dict
    

    def get_mean_sample_split(self) -> dict:
        self.reset()
        total_cache_split = 0 
        total_req_sampled = 0 
        cache_req_df = self.get_next_cache_req_group_df()
        while (len(cache_req_df)):
            total_req_sampled += 1 
            total_cache_split += len(self.get_block_req_arr(cache_req_df, 0, self._config))
            cache_req_df = self.get_next_cache_req_group_df()
        return total_cache_split/total_req_sampled
            

    def sample(
            self, 
            sample_addr_dict: dict, 
            sample_file_path: Path
    ) -> None:
        """ Sample this cache trace.

        Args:
            sample_addr_dict: Dictionary of sampled addresses.
            sample_file_path: Path of sample file to be created.
        """
        self.reset()
        with sample_file_path.open("w+") as sample_handle:
            cache_req = self.get_next_cache_req()
            while cache_req:
                if cache_req[self._config.cache_addr_header_name] in sample_addr_dict:
                    sample_handle.write("{},{},{},{},{},{}\n".format(cache_req[self._config.req_index_header_name],
                                                                        cache_req[self._config.iat_header_name],
                                                                        cache_req[self._config.cache_addr_header_name],
                                                                        cache_req[self._config.op_header_name],
                                                                        cache_req[self._config.front_misalign_header_name],
                                                                        cache_req[self._config.rear_misalign_header_name]))
                cache_req = self.get_next_cache_req()


    @staticmethod
    def create_sample_hash_file_for_addr_set(
            unique_block_addr_set: set, 
            random_seed: int, 
            sample_hash_file_path: Path
    ) -> None:
        """ Create a sample hash file.

        Args:
            random_seed: Random seed.
            sample_hash_file_path: Path of the hash file. 
        """
        hash_file = HashFile(sample_hash_file_path)
        hash_file.create(unique_block_addr_set, random_seed)


    @staticmethod
    def get_block_req_arr(
            cache_req_df: list, 
            start_time_ts: int, 
            reader_config: ReaderConfig
    ) -> List[BlockRequest]:
        """ Get block requests from a set of cache requests originating from the
        same source block request.

        Args:
            cache_req_df: DataFrame containing a set of cache requests.
            reader_config: Configuration to extract information from cache requests.
        
        Returns:
            block_req_arr: List of dictionary with attributes of each block request.
        """
        assert len(cache_req_df) > 0, "DataFrame of cache requests cannot be empty."
        block_req_arr = []
        if not cache_req_df[reader_config.op_header_name].str.contains(reader_config.write_str).any():
            cur_cache_req_df = cache_req_df
            cur_op = reader_config.read_str
        else:
            # Write requests contain misaligned read request as well if any, when generating array of block
            # request from a set of cache requests, we do not need this information so we discard the read requests.
            cur_cache_req_df = cache_req_df[cache_req_df[reader_config.op_header_name] == reader_config.write_str]
            cur_op = reader_config.write_str

        # handle the misalignment possible in the first block accessed
        cur_time_ts = start_time_ts
        first_cache_req = cur_cache_req_df.iloc[0]
        front_misalign_byte = first_cache_req[reader_config.front_misalign_header_name]
        req_start_byte = (first_cache_req[reader_config.cache_addr_header_name] * reader_config.cache_block_size_byte) + front_misalign_byte
        req_size_byte = reader_config.cache_block_size_byte - front_misalign_byte

        iat_us = first_cache_req[reader_config.iat_header_name]
        prev_key = first_cache_req[reader_config.cache_addr_header_name]
        for _, row in cur_cache_req_df.iloc[1:].iterrows():
            cur_key = row[reader_config.cache_addr_header_name]
            if cur_key - 1 == prev_key:
                # contiguous cache requests
                req_size_byte += reader_config.cache_block_size_byte
            else:
                # not contiguous cache request, meaning some block of the block request that generated
                # this set of cache requests was not sampled.
                block_req_arr.append(BlockRequest(int(cur_time_ts + iat_us),
                                                    int(req_start_byte/reader_config.lba_size_byte),
                                                    reader_config.get_write_flag(cur_op),
                                                    req_size_byte))
                req_start_byte = cur_key * reader_config.cache_block_size_byte
                req_size_byte = reader_config.cache_block_size_byte
                cur_time_ts += iat_us 
            prev_key = cur_key

        """ Include rear misalignment in the last block request generated from the set of cache requests.
        Since all cache requests in the DataFrame originate from the same block request, the rear
        misalignment can only happen in the last block which belongs to the last block request
        added to the array. """
        rear_misalign_byte = cur_cache_req_df.iloc[-1][reader_config.rear_misalign_header_name]
        block_req_arr.append(BlockRequest(int(cur_time_ts + iat_us),
                                            int(req_start_byte/reader_config.lba_size_byte),
                                            reader_config.get_write_flag(cur_op),
                                            int(req_size_byte - rear_misalign_byte)))
        
        assert all([req.size_byte>0 for req in block_req_arr]), "All sizes not greater than 0, found {}.".format(block_req_arr)
        return block_req_arr


    @staticmethod
    def get_blk_addr_arr(
            addr: int, 
            num_lower_addr_bits_ignored: int
    ) -> ndarray:
        """ Get array of blocks given the address of a region and number of lower
        address bits ignored to get to that region address.

        Args:
            addr: The address of the region.
        
        Returns:
            block_addr_arr: Array of block addresses in the region. 
        """
        num_block_in_region = 2**num_lower_addr_bits_ignored
        block_addr_arr = zeros(num_block_in_region, dtype=int)
        start_block = (addr << num_lower_addr_bits_ignored)
        for block_index in range(num_block_in_region):
            block_addr_arr[block_index] = start_block + block_index
        return block_addr_arr
    

    def close(self) -> None:
        self._handle.close()
    
    
    def reset(self) -> None:
        self._handle.seek(0)
        self._prev_cache_req = {}