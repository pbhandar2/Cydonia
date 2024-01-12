from __future__ import annotations

from pathlib import Path 
from numpy import integer, floating, ndarray
from json import dumps, JSONEncoder, load
from dataclasses import dataclass, asdict


class NpEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        if isinstance(obj, floating):
            return float(obj)
        if isinstance(obj, ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


@dataclass(frozen=True)
class BlockRequest:
    ts: int 
    lba: int
    write_flag: bool
    size_byte: int
    lba_size_byte: int = 512
    cache_block_size_byte: int = 4096

    def get_start_offset(self):
        return self.lba * self.lba_size_byte
    
    def get_end_offset(self):
        return (self.lba * self.lba_size_byte) + self.size_byte

    def get_front_misalign_byte(self):
        return self.get_start_offset() % self.cache_block_size_byte

    def get_rear_misalign_byte(self):
        return self.cache_block_size_byte - (self.get_end_offset() % self.cache_block_size_byte)

    def get_start_cache_addr(self):
        return self.get_start_offset()//self.cache_block_size_byte
    
    def get_end_cache_addr(self):
        return (self.get_end_offset()-1)//self.cache_block_size_byte


@dataclass 
class BlockStats:
    block_read_count: int = 0
    block_write_count: int = 0
    block_read_byte_sum: int = 0
    block_write_byte_sum: int = 0 
    block_read_iat_sum: int = 0
    block_write_iat_sum: int = 0

    def track(self, req: BlockRequest, prev_ts: int):
        if req.write_flag:
            self.block_write_count += 1
            self.block_write_byte_sum += req.size_byte
            self.block_write_iat_sum += (req.ts - prev_ts)
        else:
            self.block_read_count += 1
            self.block_read_byte_sum += req.size_byte
            self.block_read_iat_sum += (req.ts - prev_ts)


@dataclass 
class MisalignStats:
    misaligned_read_count: int = 0
    misaligned_write_count: int = 0
    misaligned_read_byte: int = 0
    misaligned_write_byte: int = 0
    misaligned_read_cache_req_count: int = 0
    misaligned_write_cache_req_count: int = 0

    def track(self, req: BlockRequest):
        front_misalign_byte = req.get_front_misalign_byte()
        rear_misalign_byte = req.get_rear_misalign_byte()

        start_cache_addr = req.get_start_cache_addr()
        end_cache_addr = req.get_end_cache_addr()

        if req.write_flag:
            if front_misalign_byte > 0:
                self.misaligned_write_count += 1 
                self.misaligned_write_byte += front_misalign_byte
            
            if rear_misalign_byte > 0:
                self.misaligned_write_count += 1 
                self.misaligned_write_byte += rear_misalign_byte

            if start_cache_addr == end_cache_addr:
                if front_misalign_byte > 0 or rear_misalign_byte > 0:
                    self.misaligned_write_cache_req_count += 1 
            else:
                if front_misalign_byte > 0:
                    self.misaligned_write_cache_req_count += 1 
                if rear_misalign_byte > 0:
                    self.misaligned_write_cache_req_count += 1 
        else:
            if front_misalign_byte > 0:
                self.misaligned_read_count += 1 
                self.misaligned_read_byte += front_misalign_byte
            
            if rear_misalign_byte > 0:
                self.misaligned_read_count += 1 
                self.misaligned_read_byte += rear_misalign_byte
            
            if start_cache_addr == end_cache_addr:
                if front_misalign_byte > 0 or rear_misalign_byte > 0:
                    self.misaligned_read_cache_req_count += 1 
            else:
                if front_misalign_byte > 0:
                    self.misaligned_read_cache_req_count += 1 
                if rear_misalign_byte > 0:
                    self.misaligned_read_cache_req_count += 1


class WorkloadStats:
    def __init__(
            self, 
            lba_size_byte = 512, 
            cache_block_size_byte = 4096
    ) -> None:
        self._prev_ts = None 
        self._block_stat = BlockStats()
        self._misalign_stat = MisalignStats()

        self._lba_size_byte = lba_size_byte
        self._cache_block_size_byte = cache_block_size_byte 
        

    def get_dict(self):
        """ Get WorkloadStats as a dictionary. """
        return {**asdict(self._block_stat), **asdict(self._misalign_stat)}


    def get_workload_feature_dict(self):
        """ Get basic workload features as dict. """
        block_stat, misalign_stat = self._block_stat, self._misalign_stat
        err_dict = {}
        err_dict["cur_mean_read_size"] = block_stat.block_read_byte_sum/block_stat.block_read_count if block_stat.block_read_count > 0 else 0 
        err_dict["cur_mean_write_size"] = block_stat.block_write_byte_sum/block_stat.block_write_count if block_stat.block_write_count > 0 else 0
        err_dict["cur_mean_read_iat"] = block_stat.block_read_iat_sum/block_stat.block_read_count if block_stat.block_read_count > 0 else 0 
        err_dict["cur_mean_write_iat"] = block_stat.block_write_iat_sum/block_stat.block_write_count if block_stat.block_write_count > 0 else 0
        err_dict["misalignment_per_read"] = misalign_stat.misaligned_read_count/block_stat.block_read_count if block_stat.block_read_count > 0 else 0 
        err_dict["misalignment_per_write"] = misalign_stat.misaligned_write_count/block_stat.block_write_count if block_stat.block_write_count > 0 else 0 
        return err_dict 
    

    def write_to_file(self, output_path):
        stat_dict = self.get_dict()
        with output_path.open("w+") as handle:
            handle.write(dumps(stat_dict, indent=2, cls=NpEncoder))
    

    def track(
            self,
            req: BlockRequest
    ) -> None:
        self._misalign_stat.track(req)
        if self._prev_ts is None:
            self._prev_ts = req.ts
        self._block_stat.track(req, self._prev_ts)
        self._prev_ts = req.ts


    def load_file(self, workload_stat_file: Path):
        with open(workload_stat_file, "r") as handle:
            stat_dict = load(handle)
        self.load_dict(stat_dict)
    

    def load_dict(self, stat_dict: dict):
        self._block_stat.block_read_count = stat_dict["block_read_count"]
        self._block_stat.block_write_count = stat_dict["block_write_count"]
        self._block_stat.block_read_byte_sum = stat_dict["block_read_byte_sum"]
        self._block_stat.block_write_byte_sum = stat_dict["block_write_byte_sum"]
        self._block_stat.block_read_iat_sum = stat_dict["block_read_iat_sum"]
        self._block_stat.block_write_iat_sum = stat_dict["block_write_iat_sum"]

        self._misalign_stat.misaligned_read_count = stat_dict["misaligned_read_count"]
        self._misalign_stat.misaligned_write_count = stat_dict["misaligned_write_count"]

        self._misalign_stat.misaligned_read_byte = stat_dict["misaligned_read_byte"]
        self._misalign_stat.misaligned_write_byte = stat_dict["misaligned_write_byte"]

        self._misalign_stat.misaligned_read_cache_req_count = stat_dict["misaligned_read_cache_req_count"]
        self._misalign_stat.misaligned_write_cache_req_count = stat_dict["misaligned_write_cache_req_count"]


    @staticmethod
    def update_using_cache_req_arr(stats: WorkloadStats, cache_req_arr: list):
        pass 


    def __eq__(self, other: WorkloadStats):
        return other._block_stat == self._block_stat and other._misalign_stat == self._misalign_stat