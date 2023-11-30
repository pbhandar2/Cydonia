from pathlib import Path 
from mmh3 import hash128
from numpy import zeros, ndarray


def get_blk_addr_arr(
        region_addr: int,
        num_lower_order_bits_ignored: int 
) -> ndarray:
    """Get the array of blocks in a region, whose size is determined by the number of lower
    order bits ignored, which consists of the given block address. 

    Args:
        block_addr: The address of the block to which the region belongs to. 
        num_lower_order_bits_ignored: Number of lower order bits ignored. 
    
    Returns:
        block_addr_arr: Array of block addresses in the region containing the given block address. 
    """
    num_block_in_region = 2**num_lower_order_bits_ignored
    block_addr_arr = zeros(num_block_in_region, dtype=int)
    for block_index in range(num_block_in_region):
        block_addr_arr[block_index] = (region_addr << num_lower_order_bits_ignored) + block_index
    return block_addr_arr


def generate_sample_cache_trace(
        source_cache_trace_path: Path,
        sample_blk_dict: dict,
        sample_cache_trace_path: Path 
) -> None:
    """Generate a sample cache trace given the full trace and a dictionary with sampled block addresses as keys. 

    Args:
        source_cache_trace_path: Path to the source cache trace file which will be sampled.
        sample_blk_dict: Dictionary with sampled block addresses as the key.
        sample_cache_trace_path: Path where the new sample cache trace will be generated.
    """
    with sample_cache_trace_path.open("w+") as sample_cache_trace_handle, \
            source_cache_trace_path.open("r") as source_cache_trace_handle:

        source_cache_trace_line = source_cache_trace_handle.readline().rstrip()
        while source_cache_trace_line:
            split_trace_line = source_cache_trace_line.split(",")
            if int(split_trace_line[2]) in sample_blk_dict:
                sample_cache_trace_handle.write("{}\n".format(source_cache_trace_line))
            source_cache_trace_line = source_cache_trace_handle.readline().rstrip()
    print("Generated sample cache trace {} from cache trace {}.".format(sample_cache_trace_path, source_cache_trace_path))


def get_sampled_blocks(
        trace_path: Path,
        rate: float,
        seed: int, 
        bits: int
) -> dict:
    """Get a list of sampled blocks given a cache trace, sampling rate, seed and number of lower
    bits of addresses to ignore.

    Args:
        trace_path: Path to the cache trace. 
        rate: Sampling rate between (0, 1). 
        seed: Random seed.
        bits: Number of lower bits of block addresses to ignore.
    
    Returns:
        sampled_block_dict: Dictionary with sampled block addresses as keys. 
    """
    assert rate > 0 and rate < 1, "Rate should be between (0,1) but found {}.".format(rate)

    sample_blk_dict = {}
    not_sample_block_addr_count = 0 

    max_hash_val = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    sample_limit = max_hash_val * rate 

    trace_file_handle = trace_path.open("r")
    trace_line = trace_file_handle.readline().rstrip()
    while trace_line:
        split_trace_line = trace_line.split(",")
        key= int(split_trace_line[2])
        addr = key >> bits 
        hash_val = hash128(str(addr), signed=False, seed=seed)
        if hash_val < sample_limit:
            sample_blk_dict[key] = 1 
        else:
            not_sample_block_addr_count += 1
        trace_line = trace_file_handle.readline().rstrip()

    sample_blk_count = len(sample_blk_dict.keys())
    total_blk_count = sample_blk_count + not_sample_block_addr_count
    eff_sampling_rate = sample_blk_count/total_blk_count
    print("Sampled {} blocks out of {} with eff sampling rate {}, sampling rate {}, bits {} and seed {}.".format(sample_blk_count, 
                                                                                                                    total_blk_count, 
                                                                                                                    eff_sampling_rate, 
                                                                                                                    rate,
                                                                                                                    bits,
                                                                                                                    seed))
    trace_file_handle.close()
    return sample_blk_dict