"""This file contains functions to create sample block storage traces."""

from pathlib import Path 
from mmh3 import hash128
from numpy import mean, percentile, ceil 

from cydonia.profiler.CPReader import CPReader


def sample(
        block_trace_path: str,
        rate: float,
        seed: int, 
        bits: int, 
        sample_trace_path: str,
        lba_size_byte: int = 512,
        split_percentile_arr: list = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]
) -> dict:
    """Create a block trace by sampling block requests from a block storage trace. 

    
    Args:
        block_trace_path: Path to block trace to sample. 
        rate: Rate of sampling. 
        seed: Random seed. 
        bits: The number of lower order bits of addresses to ignore. 
        sample_trace_path: Path to the new sample block trace. 
        lba_size_byte: Size of a logical block address in the block trace. 
        split_percentile_arr: Array of percentiles values to compute for split values. 
    
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
    sample_file_handle = Path(sample_trace_path).open("w+")
    reader = CPReader(block_trace_path)
    
    """A single multi-block request in the original block trace can generate multiple possibly multi-block 
    requests in the sample. This happens when a fragment of the blocks in a multi-block request are sampled. 
    This array tracks the number of sampled block request generated from each possibly multi-block request
    that is sampled. 
    """
    split_arr = []
    sampled_lba_set = set([])
    unsampled_lba_set = set([])

    block_req_count = 0 
    sample_trace_line_count = 0
    prev_sample_trace_line_count = 0 
    prev_sample_req_ts = 0 
    block_req = reader.get_next_block_req()
    prev_ts = block_req["ts"]
    while block_req:
        block_req_count += 1
        sample_block_count = 0 
        sample_start_lba = -1 

        block_req_iat = block_req["ts"] - prev_ts 
        lba_start = block_req['lba']
        lba_end = lba_start + int(block_req['size']/lba_size_byte)
        for cur_lba in range(lba_start, lba_end):
            # ignore lower order bits of the address so multiple address could map to the same value and get sampled together 
            addr = cur_lba >> bits 
            hash_val = hash128(str(addr), signed=False, seed=seed)

            if hash_val < sample_limit:
                sampled_lba_set.add(cur_lba)
                if sample_block_count == 0:
                    # its a new sample block request 
                    sample_start_lba = cur_lba
                    prev_sample_req_ts += block_req_iat
                    sample_ts = prev_sample_req_ts
                sample_block_count += 1 
            else:
                unsampled_lba_set.add(cur_lba)
                if sample_block_count == 0:
                    continue 

                assert sample_start_lba != -1, \
                    "Data error: {},{},{},{}\n".format(sample_ts, sample_start_lba, block_req["op"], int(sample_block_count * lba_size_byte))

                sample_file_handle.write("{},{},{},{}\n".format(sample_ts, sample_start_lba, block_req["op"], int(sample_block_count * lba_size_byte)))
                sample_block_count = 0 
                sample_start_lba = -1 
                sample_trace_line_count += 1 
        else:
            if sample_block_count > 0:
                assert sample_start_lba != -1, \
                    "Data error: {},{},{},{}\n".format(sample_ts, sample_start_lba, block_req["op"], int(sample_block_count * lba_size_byte))

                sample_file_handle.write("{},{},{},{}\n".format(sample_ts, sample_start_lba, block_req["op"], int(sample_block_count * lba_size_byte)))
                sample_block_count = 0 
                sample_start_lba = -1 
                sample_trace_line_count += 1 

        if sample_trace_line_count > prev_sample_trace_line_count:
            split_arr.append(sample_trace_line_count - prev_sample_trace_line_count)

        prev_sample_trace_line_count = sample_trace_line_count
        prev_ts = block_req["ts"]
        block_req = reader.get_next_block_req()
    
    sample_file_handle.close()

    sample_stats = {}
    sample_stats["rate"] = rate 
    sample_stats["seed"] = seed 
    sample_stats["bits"] = bits 
    sample_stats["split_mean"] = mean(split_arr) if len(split_arr) else 0 
    sample_stats["split_total"] = len(split_arr)     
    sample_stats["freq_%"] = 100*(sample_stats["split_total"] - split_arr.count(1))/sample_stats["split_total"] if len(split_arr) else 0 
    sample_stats["sampled_lba_count"] = len(sampled_lba_set)
    sample_stats["not_sampled_lba_count"] = len(unsampled_lba_set)
    for _, percentile_val in enumerate(split_percentile_arr):
        sample_stats['split_p_{}'.format(percentile_val)] = percentile(split_arr, percentile_val, keepdims=False) if len(split_arr) else 0 
    return sample_stats