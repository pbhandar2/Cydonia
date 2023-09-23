"""This file contains functions to create sample block storage traces."""

from pathlib import Path 
from mmh3 import hash128
from numpy import mean, percentile, ceil 
from pandas import DataFrame, read_csv

from cydonia.profiler.CPReader import CPReader


def create_sample_trace(
        block_trace_df: DataFrame, 
        sampled_lba_dict: dict,
        sample_trace_path: Path, 
        lba_size_byte: int = 512,
        split_percentile_arr: list = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]
) -> dict:
    """Create a sample block trace by sampling the LBA matching the keys of the sampling dictionary. 

    Args:
        block_trace_df: DataFrame of full block trace. 
        sampled_lba_dict: Dictionary with sampled LBA as keys.
        sample_trace_path: Path where sample block trace is generated. 
        lba_size_byte: Size of each LBA in bytes. 
        split_percentile_arr: Array of percentiles values to compute for split values. 
    
    Returns:
        split_stat_dict: Dictionary with statistics regarding the number of sample block request generated per block request
                            sampled. 
    """
    # track the previous timestamp to generate IAT 
    prev_ts = int(block_trace_df.iloc[0]["ts"])
    sample_req_count, prev_sample_req_count = 0, 0 
    sample_split_arr = []
    prev_sample_req_ts = 0 
    sample_file_handle = sample_trace_path.open("w+")
    for _, row in block_trace_df.iterrows():
        ts, lba, op, size = int(row["ts"]), int(row["lba"]), row["op"], int(row["size"])
        block_req_iat = ts - prev_ts 
        lba_end = lba + int(size/lba_size_byte)

        sample_block_count = 0 
        sample_start_lba = -1 

        for cur_lba in range(lba, lba_end):
            if cur_lba in sampled_lba_dict:
                if sample_block_count == 0:
                    sample_start_lba = cur_lba
                    prev_sample_req_ts += block_req_iat
                sample_block_count += 1 
            else:
                if sample_block_count == 0:
                    continue 

                sample_req_size_byte = int(sample_block_count * lba_size_byte)
                assert sample_req_size_byte < size, "Sample req size cannot be larger than block req size"
                sample_file_handle.write("{},{},{},{}\n".format(prev_sample_req_ts, sample_start_lba, op, sample_req_size_byte))
                sample_block_count = 0 
                sample_start_lba = -1 
                sample_req_count += 1
        else:
            if sample_block_count > 0:
                assert sample_start_lba != -1, \
                    "Data error: {},{},{},{}\n".format(prev_sample_req_ts, sample_start_lba, op, int(sample_block_count * lba_size_byte))

                sample_req_size_byte = int(sample_block_count * lba_size_byte)
                assert sample_req_size_byte <= size, "Sample req size cannot be larger than block req size"
                sample_file_handle.write("{},{},{},{}\n".format(prev_sample_req_ts, sample_start_lba, op, int(sample_block_count * lba_size_byte)))
                sample_req_count += 1
                sample_block_count = 0 
                sample_start_lba = -1 
        
        if sample_req_count > prev_sample_req_count:
            sample_split_arr.append(sample_req_count - prev_sample_req_count)
        
        prev_ts = ts 
        prev_sample_req_count = sample_req_count
    
    sample_file_handle.close() 
    split_stats = {
        "split_total": len(sample_split_arr),
        "split_mean": mean(sample_split_arr) if len(sample_split_arr) else 0,
        "freq_%": 100*(len(sample_split_arr) - sample_split_arr.count(1))/len(sample_split_arr) if len(sample_split_arr) else 0 
    }
    for _, percentile_val in enumerate(split_percentile_arr):
        split_stats['split_p_{}'.format(percentile_val)] = percentile(sample_split_arr, percentile_val, keepdims=False) if len(sample_split_arr) else 0 

    return split_stats


def sample(
        block_trace_path: str,
        rate: float,
        seed: int, 
        bits: int, 
        sample_trace_path: str,
        lba_size_byte: int = 512
) -> dict:
    """Create a block trace by sampling block requests from a block storage trace. 

    Args:
        block_trace_path: Path to block trace to sample. 
        rate: Rate of sampling. 
        seed: Random seed. 
        bits: The number of lower order bits of addresses to ignore. 
        sample_trace_path: Path to the new sample block trace. 
        lba_size_byte: Size of a logical block address in the block trace. 
    
    Returns:
        sampling_stats: Dictionary of statistics related to the sampling process. 
    
    Raises:
        ValueError: Raised if block trace path doesn't exist or rate is less than 1. 
        AssertionError: Raised if LBA of sampled block trace is invalid (less than zero). 
    """
    if not Path(block_trace_path).exists():
        raise ValueError("Block trace path {} does not exists.".format(block_trace_path))

    if rate >= 1:
        raise ValueError("Sampling rate has to be less than 1, but found {}.".format(rate))

    max_hash_val = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    sample_limit = max_hash_val * rate 
    reader = CPReader(block_trace_path)
    sample_lba_dict = {}
    unsampled_lba_dict = {}

    block_req = reader.get_next_block_req()
    while block_req:
        lba_start = block_req['lba']
        lba_end = lba_start + int(block_req['size']/lba_size_byte)
        for cur_lba in range(lba_start, lba_end):
            addr = cur_lba >> bits 
            hash_val = hash128(str(addr), signed=False, seed=seed)
            if hash_val < sample_limit:
                sample_lba_dict[cur_lba] = 1 
            else:
                unsampled_lba_dict[cur_lba] = 1
        block_req = reader.get_next_block_req()
    
    trace_df = read_csv(block_trace_path, names=["ts", "lba", "op", "size"])
    sample_stat_dict = create_sample_trace(trace_df, sample_lba_dict, Path(sample_trace_path))
    sample_stat_dict["rate"] = rate 
    sample_stat_dict["seed"] = seed 
    sample_stat_dict["bits"] = bits 
    sample_stat_dict["sampled_lba_count"] = len(sample_lba_dict.keys())
    sample_stat_dict["not_sampled_lba_count"] = len(unsampled_lba_dict.keys())
    return sample_stat_dict