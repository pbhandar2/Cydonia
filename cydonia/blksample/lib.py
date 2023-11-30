from copy import deepcopy
from pathlib import Path
from time import perf_counter_ns 
from pandas import read_csv 
from pandas import DataFrame
from numpy import mean, std, ndarray, zeros

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap

DEFAULT_WORKLOAD_FEATURES_TO_COMPARE_ARR = ["write_block_req_split", 
                                                "write_cache_req_split", 
                                                "iat_read_avg", 
                                                "iat_write_avg", 
                                                "read_size_avg", 
                                                "write_size_avg"]


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


def get_feature_err_dict(
        full_workload_feature_dict: dict,
        sample_workload_feature_dict: dict,
        workload_feature_to_compare_arr: list =DEFAULT_WORKLOAD_FEATURES_TO_COMPARE_ARR
) -> dict:
    error_dict = {}
    total_error_arr = []
    for feature_name in workload_feature_to_compare_arr:
        error_dict[feature_name] = 100*abs(full_workload_feature_dict[feature_name] - sample_workload_feature_dict[feature_name])/full_workload_feature_dict[feature_name]
        total_error_arr.append(error_dict[feature_name])
    error_dict["mean"] = mean(total_error_arr)
    error_dict["std"] = std(total_error_arr)
    return error_dict


def get_unique_block_count(cache_trace_path: Path):
    cache_trace_df = load_cache_trace(cache_trace_path)
    return len(cache_trace_df["key"].unique())


def load_cache_trace(cache_trace_path: Path) -> DataFrame:
    return read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"]) 
