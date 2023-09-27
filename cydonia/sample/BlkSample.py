"""This file contains functions to post process block trace samples."""

from pathlib import Path
from copy import deepcopy
from time import perf_counter_ns
from pandas import DataFrame, read_csv 
from numpy import ndarray, zeros, mean, std 

from cydonia.sample.Sample import create_sample_trace
from cydonia.sample.BlkReq import BlkReq, FirstTwoBlkReqTracker


def load_remove_algorithm_metadata(
        df: DataFrame,
        block_size_byte: int = 512 
) -> tuple:
    """Get a tuple of a dictionary of access statistics of each block and a FirstTwoBlkReqTracker
    object that tracks the first and second block request as blocks are removed from the block trace. 

    Args:
        df: DataFrame of block trace. 
        block_size_byte: Size of block in byte. 
    
    Returns:
        access_stat_dict, first_blk_req_tracker: Access statistics of each block and a FirstTwoBlkReqTracker object that 
                                                    tracks the first and second block request as blocks are removed from 
                                                    the block trace. 
    """
    access_stat_dict = {}
    # Everytime, we find a block request where we see a block for the first time, we add to this tracker. 
    # Using that information, we can always track what the first two block request of the sample would
    # be if a given block were to be removed. 
    first_blk_req_tracker = FirstTwoBlkReqTracker()

    # track the number of request and unique blocks at each point in the trace 
    req_count_tracker = 0 
    blk_count_tracker = 0 
    for _, row in df.iterrows():
        req_count_tracker += 1
        blk_addr, size_byte, op, ts = int(row["lba"]), int(row["size"]), row["op"], int(row["ts"])
        size_block = size_byte//block_size_byte
        assert size_byte % 512 == 0 and size_block > 0 
        # first req has infinite IAT, setting it to 0 
        try:
            cur_iat = int(row["iat"])
        except ValueError:
            cur_iat = 0

        # We have to track solo requests separately, so this is a solo request, track and continue. 
        if size_block == 1:
            if blk_addr not in access_stat_dict:
                blk_count_tracker += 1
                access_stat_dict[blk_addr] = dict(init_access_stat_dict())
                # If a new block was found in this block request, add it to the FirstTwoBlkReqTracker object. 
                first_blk_req_tracker.add_blk_req(BlkReq(blk_addr, size_block, op, blk_count_tracker, cur_iat))
                
            access_stat_dict[blk_addr]["{}_solo_count".format(op)] += 1 
            access_stat_dict[blk_addr]["{}_solo_iat_sum".format(op)] += cur_iat
            continue 

        start_lba = blk_addr
        end_lba = start_lba + size_block
        new_blk_req_found = False 
        for cur_lba in range(start_lba, end_lba):
            if cur_lba not in access_stat_dict:
                blk_count_tracker += 1
                access_stat_dict[cur_lba] = dict(init_access_stat_dict())
                new_blk_req_found = True 

            if cur_lba == start_lba:
                access_stat_dict[cur_lba]["{}_left_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_left_iat_sum".format(op)] += cur_iat
            elif cur_lba == start_lba+size_block-1:
                access_stat_dict[cur_lba]["{}_right_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_right_iat_sum".format(op)] += cur_iat
            else:
                access_stat_dict[cur_lba]["{}_mid_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_mid_iat_sum".format(op)] += cur_iat
        
        # If a new block was found in this block request, add it to the FirstTwoBlkReqTracker object. 
        if new_blk_req_found:
            first_blk_req_tracker.add_blk_req(BlkReq(blk_addr, size_block, op, blk_count_tracker, cur_iat))

    first_blk_req_tracker.load_arr()
    return access_stat_dict, first_blk_req_tracker


def get_err_df_on_remove(
        per_blk_access_stat_dict: dict, 
        first_blk_req_tracker: FirstTwoBlkReqTracker,
        sample_workload_stat_dict: dict, 
        workload_stat_dict: dict,
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512
) -> DataFrame:
    """For each block in the trace, compute the error if the block is removed and return the information as a DataFrame. 

    Args:
        per_blk_access_stat_dict: Dictionary of access statistics of each block in the trace. 
        first_blk_req_arr: Array of first block request of each block in order of appearence in trace. 
        sample_workload_stat_dict: Dictionary of workload features of the sample. 
        workload_stat_dict: Dictionary of workload features of the full trace. 
        num_lower_order_bits_ignored: Number of lower order bits of block addresses ignored to increase sampling granularity.
        blk_size_byte: Size of each block in byte. 
    
    Returns:
        err_df: DataFrame of error values if each block were to be removed from the sample. 
    """
    error_dict_arr = []
    region_addr_arr = list(set([block_addr >> num_lower_order_bits_ignored for block_addr in per_blk_access_stat_dict]))
    for _, region_addr in enumerate(region_addr_arr):
        region_blk_addr_arr = get_blk_addr_arr(region_addr, num_lower_order_bits_ignored)

        # we need a copy of FirstTwoBlkReqTracker so that we do not update the metadata in the main object 
        first_blk_req_tracker_copy = first_blk_req_tracker.copy()

        # any block with udpated stats will be stored in this dict 
        new_per_blk_access_stat_dict = {}
        new_workload_stat_dict = deepcopy(sample_workload_stat_dict)
        for blk_addr in region_blk_addr_arr:
            if blk_addr not in per_blk_access_stat_dict:
                continue 

            if blk_addr in new_per_blk_access_stat_dict:
                cur_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr]
            else:
                cur_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr]

            right_blk_access_stat_dict = {}
            if blk_addr+1 in new_per_blk_access_stat_dict:
                right_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr+1]
            elif blk_addr+1 in per_blk_access_stat_dict:
                right_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr+1]

            left_blk_access_stat_dict = {}
            if blk_addr-1 in new_per_blk_access_stat_dict:
                left_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr-1]
            elif blk_addr-1 in per_blk_access_stat_dict:
                left_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr-1]
            
            return_tuple = remove_blk(blk_addr, 
                                        new_workload_stat_dict, 
                                        cur_blk_access_stat_dict,
                                        left_blk_access_stat_dict,
                                        right_blk_access_stat_dict,
                                        first_blk_req_tracker_copy,
                                        blk_size_byte=blk_size_byte)
            
            new_workload_stat_dict, new_left_blk_access_stat_dict, new_right_blk_access_stat_dict = return_tuple
            if new_left_blk_access_stat_dict:
                new_per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
            
            if new_right_blk_access_stat_dict:
                new_per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict 

        percent_error_dict = get_percent_error_dict(workload_stat_dict, new_workload_stat_dict)
        percent_error_dict["region"] = region_addr
        error_dict_arr.append(percent_error_dict)

    return DataFrame(error_dict_arr)


def blk_unsample(
        sample_df: DataFrame,
        workload_stat_dict: dict,
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512,
        test_mode: bool = False,
        test_trace_path: Path = Path("/")
) -> DataFrame:
    """Reduce sample workload feature error by unsampling (removing blocks).

    Args:
        sample_df: DataFrame of sample block trace. 
        workload_stat_dict: Dictionary of statistics of full workload features. 
        num_lower_order_bits_ignored: Number of lower order bits ignored. 
        blk_size_byte: Size of block in byte. 
    """
    start_time_ns = perf_counter_ns()
    per_blk_access_stat_dict, first_blk_req_tracker = load_remove_algorithm_metadata(sample_df)

    sample_workload_stat_dict = get_workload_stat_dict(sample_df)
    remove_error_df = get_err_df_on_remove(per_blk_access_stat_dict, 
                                            first_blk_req_tracker,
                                            sample_workload_stat_dict, 
                                            workload_stat_dict,
                                            num_lower_order_bits_ignored=num_lower_order_bits_ignored)

    percent_err_dict_arr = []
    new_per_blk_access_stat_dict = {}
    new_workload_stat_dict = deepcopy(sample_workload_stat_dict)
    while len(remove_error_df):
        best_row = remove_error_df.sort_values(by=["mean"]).iloc[0]
        region_addr = int(best_row["region"])

        region_blk_addr_arr = get_blk_addr_arr(int(region_addr), num_lower_order_bits_ignored)
        for blk_addr in region_blk_addr_arr:
            if blk_addr not in per_blk_access_stat_dict:
                continue 

            if blk_addr in new_per_blk_access_stat_dict:
                cur_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr]
            else:
                cur_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr]

            right_blk_access_stat_dict = {}
            if blk_addr+1 in new_per_blk_access_stat_dict:
                right_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr+1]
            elif blk_addr+1 in per_blk_access_stat_dict:
                right_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr+1]

            left_blk_access_stat_dict = {}
            if blk_addr-1 in new_per_blk_access_stat_dict:
                left_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr-1]
            elif blk_addr-1 in per_blk_access_stat_dict:
                left_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr-1]
            
            return_tuple = remove_blk(blk_addr, 
                                        new_workload_stat_dict, 
                                        cur_blk_access_stat_dict,
                                        left_blk_access_stat_dict,
                                        right_blk_access_stat_dict,
                                        first_blk_req_tracker,
                                        blk_size_byte=blk_size_byte)

            new_workload_stat_dict, new_left_blk_access_stat_dict, new_right_blk_access_stat_dict= return_tuple
            if new_left_blk_access_stat_dict:
                new_per_blk_access_stat_dict[blk_addr-1] = deepcopy(new_left_blk_access_stat_dict)
            
            if new_right_blk_access_stat_dict:
                new_per_blk_access_stat_dict[blk_addr+1] = deepcopy(new_right_blk_access_stat_dict)

            per_blk_access_stat_dict.pop(blk_addr)
            
            if blk_addr in new_per_blk_access_stat_dict:
                new_per_blk_access_stat_dict.pop(blk_addr)
            
            if new_left_blk_access_stat_dict:
                per_blk_access_stat_dict[blk_addr-1] = deepcopy(new_left_blk_access_stat_dict)

            if new_right_blk_access_stat_dict:
                per_blk_access_stat_dict[blk_addr+1] = deepcopy(new_right_blk_access_stat_dict)

        if test_mode:
            sample_lba_dict = dict.fromkeys(per_blk_access_stat_dict.keys(), 1)
            create_sample_trace(sample_df, sample_lba_dict, test_trace_path)
            new_sample_df = load_blk_trace(test_trace_path)
            new_sample_workload_stat_dict = get_workload_stat_dict(new_sample_df)
            for key in new_workload_stat_dict:                    
                assert new_workload_stat_dict[key] == new_sample_workload_stat_dict[key],\
                    "Key {} not matching for region {} in  new compute dict {} and new file dict {}".format(key, region_addr, new_workload_stat_dict, new_sample_workload_stat_dict)
            print("Test passed for region {}, {} remaining.".format(region_addr, len(remove_error_df)))

        percent_error_dict = get_percent_error_dict(workload_stat_dict, new_workload_stat_dict)
        percent_error_dict["region"] = region_addr
        percent_error_dict["time_elapsed_ns"] = perf_counter_ns() - start_time_ns 
        percent_err_dict_arr.append(percent_error_dict)
        print("Remaining regions: {}\n Current error stats: {}".format(len(remove_error_df), percent_error_dict))
        
        remove_error_df = get_err_df_on_remove(per_blk_access_stat_dict, 
                                                first_blk_req_tracker, 
                                                new_workload_stat_dict, 
                                                workload_stat_dict,
                                                num_lower_order_bits_ignored=num_lower_order_bits_ignored)
    return DataFrame(percent_err_dict_arr)


def remove_blk(
        blk_addr: int, 
        workload_stat_dict: dict, 
        blk_access_stat_dict: dict,
        left_blk_access_stat_dict: dict,
        right_blk_access_stat_dict: dict,
        first_blk_req_tracker: FirstTwoBlkReqTracker,
        blk_size_byte: int = 512
) -> tuple:
    new_sub_remove_stat_dict = get_new_sub_remove_stat_dict(blk_access_stat_dict, left_blk_access_stat_dict is not None, right_blk_access_stat_dict is not None)
    new_workload_stat_dict = deepcopy(workload_stat_dict)
    for stat_key in new_sub_remove_stat_dict:
        if 'r' == stat_key[0] and "count" in stat_key:
            new_workload_stat_dict["total_read_size"] -= (blk_size_byte * new_sub_remove_stat_dict[stat_key])
        elif 'w' == stat_key[0] and "count" in stat_key:
            new_workload_stat_dict["total_write_size"] -= (blk_size_byte * new_sub_remove_stat_dict[stat_key])
        
    new_workload_stat_dict["read_count"] += new_sub_remove_stat_dict["r_new_count"]
    new_workload_stat_dict["total_read_iat"] += new_sub_remove_stat_dict["r_new_iat_sum"]

    new_workload_stat_dict["write_count"] += new_sub_remove_stat_dict["w_new_count"]
    new_workload_stat_dict["total_write_iat"] += new_sub_remove_stat_dict["w_new_iat_sum"]

    new_workload_stat_dict["read_count"] -= new_sub_remove_stat_dict["r_remove_count"]
    new_workload_stat_dict["total_read_iat"] -= new_sub_remove_stat_dict["r_remove_iat_sum"]

    new_workload_stat_dict["write_count"] -= new_sub_remove_stat_dict["w_remove_count"]
    new_workload_stat_dict["total_write_iat"] -= new_sub_remove_stat_dict["w_remove_iat_sum"]

    if first_blk_req_tracker.is_first_solo_req(blk_addr):
        first_blk_req, second_blk_req =  first_blk_req_tracker._first_blk_req, first_blk_req_tracker._second_blk_req
        first_blk_req_iat = first_blk_req.iat 
        if second_blk_req.op == 'r':
            new_workload_stat_dict["total_read_iat"] -= (second_blk_req.iat - first_blk_req.iat) \
                                                            if not second_blk_req.is_empty() else 0
            if new_workload_stat_dict["total_read_iat"] < 0:
                print(first_blk_req)
                print(second_blk_req)
                print(new_workload_stat_dict["total_read_iat"])
        elif second_blk_req.op == 'w':
            new_workload_stat_dict["total_write_iat"] -= (second_blk_req.iat - first_blk_req.iat) \
                                                            if not second_blk_req.is_empty() else 0
            if new_workload_stat_dict["total_write_iat"] < 0:
                print(first_blk_req)
                print(second_blk_req)
                print(new_workload_stat_dict["total_read_iat"])
        else:
            new_workload_stat_dict["total_read_size"] = 0
            new_workload_stat_dict["total_write_size"] = 0
            new_workload_stat_dict["total_read_iat"] = 0
            new_workload_stat_dict["total_write_iat"] = 0
    first_blk_req_tracker.remove(blk_addr)

    
    new_workload_stat_dict["write_ratio"] = new_workload_stat_dict["write_count"]/(new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"]) \
                                                if (new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"]) > 0 else 0 
    
    new_workload_stat_dict["mean_read_size"] = new_workload_stat_dict["total_read_size"]/new_workload_stat_dict["read_count"] \
                                                    if new_workload_stat_dict["read_count"] > 0 else 0 

    new_workload_stat_dict["mean_write_size"] = new_workload_stat_dict["total_write_size"]/new_workload_stat_dict["write_count"] \
                                                    if new_workload_stat_dict["write_count"] > 0 else 0 
    
    new_workload_stat_dict["mean_read_iat"] = new_workload_stat_dict["total_read_iat"]/new_workload_stat_dict["read_count"] \
                                                    if new_workload_stat_dict["read_count"] > 0 else 0 
    
    new_workload_stat_dict["mean_write_iat"] = new_workload_stat_dict["total_write_iat"]/new_workload_stat_dict["write_count"] \
                                                    if new_workload_stat_dict["write_count"] > 0 else 0 

    copy_left_blk_access_stat_dict = deepcopy(left_blk_access_stat_dict)
    if left_blk_access_stat_dict:
        copy_left_blk_access_stat_dict["r_right_count"] += copy_left_blk_access_stat_dict["r_mid_count"]
        copy_left_blk_access_stat_dict["w_right_count"] += copy_left_blk_access_stat_dict["w_mid_count"]
        copy_left_blk_access_stat_dict["r_right_iat_sum"] += copy_left_blk_access_stat_dict["r_mid_iat_sum"]
        copy_left_blk_access_stat_dict["w_right_iat_sum"] += copy_left_blk_access_stat_dict["w_mid_iat_sum"]
        copy_left_blk_access_stat_dict["r_mid_count"], copy_left_blk_access_stat_dict["w_mid_count"] = 0, 0 
        copy_left_blk_access_stat_dict["r_mid_iat_sum"], copy_left_blk_access_stat_dict["w_mid_iat_sum"] = 0, 0 

        # all request of the left block where it was the left most block now tuns into a solo access 
        copy_left_blk_access_stat_dict["r_solo_count"] += copy_left_blk_access_stat_dict["r_left_count"]
        copy_left_blk_access_stat_dict["w_solo_count"] += copy_left_blk_access_stat_dict["w_left_count"]
        copy_left_blk_access_stat_dict["r_solo_iat_sum"] += copy_left_blk_access_stat_dict["r_left_iat_sum"]
        copy_left_blk_access_stat_dict["w_solo_iat_sum"] += copy_left_blk_access_stat_dict["w_left_iat_sum"]
        copy_left_blk_access_stat_dict["r_left_count"], copy_left_blk_access_stat_dict["w_left_count"] = 0, 0 
        copy_left_blk_access_stat_dict["r_left_iat_sum"], copy_left_blk_access_stat_dict["w_left_iat_sum"] = 0, 0 
    
    copy_right_blk_access_stat_dict = deepcopy(right_blk_access_stat_dict)
    if right_blk_access_stat_dict:
        copy_right_blk_access_stat_dict["r_left_count"] += copy_right_blk_access_stat_dict["r_mid_count"]
        copy_right_blk_access_stat_dict["w_left_count"] += copy_right_blk_access_stat_dict["w_mid_count"]
        copy_right_blk_access_stat_dict["r_left_iat_sum"] += copy_right_blk_access_stat_dict["r_mid_iat_sum"]
        copy_right_blk_access_stat_dict["w_left_iat_sum"] += copy_right_blk_access_stat_dict["w_mid_iat_sum"]
        copy_right_blk_access_stat_dict["r_mid_count"], copy_right_blk_access_stat_dict["w_mid_count"] = 0, 0 
        copy_right_blk_access_stat_dict["r_mid_iat_sum"], copy_right_blk_access_stat_dict["w_mid_iat_sum"] = 0, 0 

        # all request of the right block where it was the right most block now tuns into a solo access 
        copy_right_blk_access_stat_dict["r_solo_count"] += copy_right_blk_access_stat_dict["r_right_count"]
        copy_right_blk_access_stat_dict["w_solo_count"] += copy_right_blk_access_stat_dict["w_right_count"]
        copy_right_blk_access_stat_dict["r_solo_iat_sum"] += copy_right_blk_access_stat_dict["r_right_iat_sum"]
        copy_right_blk_access_stat_dict["w_solo_iat_sum"] += copy_right_blk_access_stat_dict["w_right_iat_sum"]
        copy_right_blk_access_stat_dict["r_right_count"], copy_right_blk_access_stat_dict["w_right_count"] = 0, 0 
        copy_right_blk_access_stat_dict["r_right_iat_sum"], copy_right_blk_access_stat_dict["w_right_iat_sum"] = 0, 0 

    return new_workload_stat_dict, copy_left_blk_access_stat_dict, copy_right_blk_access_stat_dict


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


def get_percent_error_dict(
        full_stat_dict: dict, 
        sample_stat_dict: dict
) -> dict:
    """Get dictionary of percent error from dictionary of full and sample trace stats. 
    
    Args:
        self._full_stat_dict: Dictionary of full trace stats. 
        self._sample_stat_dict: Dictionary of sample trace stats. 
    
    Returns:
        percent_error_dict: Dictionary of percent error of select features. 
    """
    percent_error_dict = {}
    
    full_mean_read_size = full_stat_dict["total_read_size"]/full_stat_dict["read_count"] \
                            if full_stat_dict["read_count"] > 0 else 0
    
    sample_mean_read_size = sample_stat_dict["total_read_size"]/sample_stat_dict["read_count"] \
                                if sample_stat_dict["read_count"] > 0 else 0

    percent_error_dict["mean_read_size"] = 100.0*(full_mean_read_size - sample_mean_read_size)/full_mean_read_size \
                                                if full_mean_read_size > 0 else 0

    full_mean_write_size = full_stat_dict["total_write_size"]/full_stat_dict["write_count"] \
                                if full_stat_dict["write_count"] > 0 else 0

    sample_mean_write_size = sample_stat_dict["total_write_size"]/sample_stat_dict["write_count"] \
                                if sample_stat_dict["write_count"] > 0 else 0

    percent_error_dict["mean_write_size"] = 100.0*(full_mean_write_size - sample_mean_write_size)/full_mean_write_size \
                                                if full_mean_write_size > 0 else 0

    full_mean_read_iat = full_stat_dict["total_read_iat"]/full_stat_dict["read_count"] \
                            if full_stat_dict["read_count"] > 0 else 0

    sample_mean_read_iat = sample_stat_dict["total_read_iat"]/sample_stat_dict["read_count"] \
                                if sample_stat_dict["read_count"] > 0 else 0

    percent_error_dict["mean_read_iat"] = 100.0*(full_mean_read_iat - sample_mean_read_iat)/full_mean_read_iat \
                                            if full_mean_read_iat > 0 else 0 

    full_mean_write_iat = full_stat_dict["total_write_iat"]/full_stat_dict["write_count"] \
                            if full_stat_dict["write_count"] > 0 else 0 

    sample_mean_write_iat = sample_stat_dict["total_write_iat"]/sample_stat_dict["write_count"] \
                                if sample_stat_dict["write_count"] > 0 else 0 
    
    percent_error_dict["mean_write_iat"] = 100.0*(full_mean_write_iat - sample_mean_write_iat)/full_mean_write_iat \
                                                if full_mean_write_iat > 0 else 0 

    full_write_ratio = full_stat_dict["write_count"]/(full_stat_dict["read_count"] + full_stat_dict["write_count"]) \
                            if (full_stat_dict["read_count"] + full_stat_dict["write_count"]) > 0 else 0 

    sample_write_ratio = sample_stat_dict["write_count"]/(sample_stat_dict["read_count"] + sample_stat_dict["write_count"]) \
                            if (sample_stat_dict["read_count"] + sample_stat_dict["write_count"]) > 0 else 0 

    percent_error_dict["write_ratio"] = 100.0 * (full_write_ratio - sample_write_ratio)/full_write_ratio \
                                            if full_write_ratio > 0 else 0 

    mean_err = mean(list([abs(_) for _ in percent_error_dict.values()]))
    std_dev = std(list([abs(_) for _ in percent_error_dict.values()]))

    percent_error_dict["mean"] = mean_err 
    percent_error_dict["std"] = std_dev
    return percent_error_dict


def get_new_sub_remove_stat_dict(
        access_feature_dict: dict, 
        left_sampled: bool,
        right_sampled: bool
) -> dict:
    """Get new, sub and remove events from removing a block with the given access statistics. 

    Args:
        access_feature_dict: Dictionary of access features of the block being removed. 
        left_sampled: Boolean indicating if left block is sampled. 
        right_sampled: Boolean indicating if right block is sampled. 
    
    Returns:
        new_sub_remove_stat_dict: Dictionary of new, sub and remove statisitcs from removing the given block. 
    """
    read_remove_count, write_remove_count = access_feature_dict["r_solo_count"], access_feature_dict["w_solo_count"]
    read_remove_iat_sum, write_remove_iat_sum = access_feature_dict["r_solo_iat_sum"], access_feature_dict["w_solo_iat_sum"]

    read_sub_count, write_sub_count = 0, 0 
    read_sub_iat_sum, write_sub_iat_sum = 0, 0 
    read_new_count, write_new_count = 0, 0 
    read_new_iat_sum, write_new_iat_sum = 0, 0 

    if left_sampled and right_sampled:
        read_new_count += access_feature_dict["r_mid_count"]
        read_new_iat_sum += access_feature_dict["r_mid_iat_sum"]
        write_new_count += access_feature_dict["w_mid_count"]
        write_new_iat_sum += access_feature_dict["w_mid_iat_sum"]

        read_sub_count += (access_feature_dict["r_left_count"]+access_feature_dict["r_right_count"])
        read_sub_iat_sum += (access_feature_dict["r_left_iat_sum"]+access_feature_dict["r_right_iat_sum"])
        write_sub_count += (access_feature_dict["w_left_count"]+access_feature_dict["w_right_count"])
        write_sub_iat_sum += (access_feature_dict["w_left_iat_sum"]+access_feature_dict["w_right_iat_sum"])
    
    elif left_sampled and not right_sampled:
        """The block to the left is sampled, to the right isn't. This means we cannot have any accesses
        where this block is the middle block. We also cannot have any accesses where this block is the left
        most block since that access would require the block to its right also to be accessed. The requests
        where this block is the rightmost block would see a reduction of size with no impact on interarrival
        time of the trace. 
        """
        assert (access_feature_dict["r_left_count"] == 0 and access_feature_dict["r_left_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."

        assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."
        
        read_sub_count += access_feature_dict["r_right_count"]
        read_sub_iat_sum += access_feature_dict["r_right_iat_sum"]
        write_sub_count += access_feature_dict["w_right_count"]
        write_sub_iat_sum += access_feature_dict["w_right_iat_sum"]
    
    elif not left_sampled and right_sampled:
        assert (access_feature_dict["r_right_count"] == 0 and access_feature_dict["r_right_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."

        assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
            "If the left block is not sampled, then there cannot be any block request where this is the left most block."
        
        read_sub_count += access_feature_dict["r_left_count"]
        read_sub_iat_sum += access_feature_dict["r_left_iat_sum"]
        write_sub_count += access_feature_dict["w_left_count"]
        write_sub_iat_sum += access_feature_dict["w_left_iat_sum"]
    
    else:
        assert (access_feature_dict["r_right_count"] == 0 and access_feature_dict["r_right_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."

        assert (access_feature_dict["r_left_count"] == 0 and access_feature_dict["r_left_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."

        assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
            "If the right block is not sampled, then there cannot be any block request where this is the left most block."

    return {
        "r_new_count": read_new_count,
        "w_new_count": write_new_count,
        "r_new_iat_sum": read_new_iat_sum,
        "w_new_iat_sum": write_new_iat_sum,
        "r_sub_count": read_sub_count,
        "w_sub_count": write_sub_count,
        "r_sub_iat_sum": read_sub_iat_sum,
        "w_sub_iat_sum": write_sub_iat_sum,
        "r_remove_count": read_remove_count,
        "w_remove_count": write_remove_count,
        "r_remove_iat_sum": read_remove_iat_sum,
        "w_remove_iat_sum": write_remove_iat_sum
    }


def get_workload_stat_dict(df: DataFrame) -> dict:
    """Get dictionary of workload statistics from a DataFrame of a block trace. 
    
    Args:
        df: DataFrame of a block trace. 
    
    Returns:
        stat_dict: Dictionary of workload features. 
    """
    stat_dict = {}
    stat_dict["read_count"] = len(df[df['op']=='r'])
    stat_dict["write_count"] = len(df[df['op']=='w'])
    stat_dict["total_read_size"] = df[df['op']=='r']['size'].sum()
    stat_dict["total_write_size"] =  df[df['op']=='w']['size'].sum()
    stat_dict["total_read_iat"] = df[df['op']=='r']['iat'].sum()
    stat_dict["total_write_iat"] = df[df['op']=='w']['iat'].sum()

    stat_dict["write_ratio"] = stat_dict["write_count"]/(stat_dict["read_count"] + stat_dict["write_count"]) \
                                    if (stat_dict["read_count"] + stat_dict["write_count"]) > 0 else 0 

    stat_dict["mean_read_size"] = stat_dict["total_read_size"]/stat_dict["read_count"] \
                                    if stat_dict["read_count"] > 0 else 0 

    stat_dict["mean_write_size"] = stat_dict["total_write_size"]/stat_dict["write_count"] \
                                        if stat_dict["write_count"] > 0 else 0 

    stat_dict["mean_read_iat"] = stat_dict["total_read_iat"]/stat_dict["read_count"] \
                                    if stat_dict["read_count"] > 0 else 0 
    
    stat_dict["mean_write_iat"] = stat_dict["total_write_iat"]/stat_dict["write_count"] \
                                    if stat_dict["write_count"] > 0 else 0 

    return stat_dict 


def init_access_stat_dict(
) -> dict:
    """Get the template for LBA access dict. 

    Returns:
        access_dict: Dictionary with all keys of LBA stats initiated to 0. 
    """
    return {
        "r_solo_count": 0,
        "w_solo_count": 0,
        "r_solo_iat_sum": 0, 
        "w_solo_iat_sum": 0,

        "r_right_count": 0,
        "w_right_count": 0,
        "r_right_iat_sum": 0, 
        "w_right_iat_sum": 0, 

        "r_left_count": 0,
        "w_left_count": 0,
        "r_left_iat_sum": 0, 
        "w_left_iat_sum": 0, 

        "r_mid_count": 0,
        "w_mid_count": 0,
        "r_mid_iat_sum": 0, 
        "w_mid_iat_sum": 0
    }


def load_blk_trace(
    trace_path: Path
) -> DataFrame:
    """Load a block trace file into a pandas DataFrame.  
    
    Args:
        trace_path: Path to block trace. 
    
    Returns:
        df: Block trace with additional features as a DataFrame. 
    """
    df = read_csv(trace_path, names=["ts", "lba", "op", "size"])
    df["iat"] = df["ts"].diff()
    df["iat"] = df["iat"].fillna(0)
    return df 


def get_unique_blk_addr_set(
        df: DataFrame, 
        blk_size_byte: int = 512
) -> set:
    blk_addr_set = set()
    total_line = len(df)
    line_count = 0 
    for index, row in df.iterrows():
        line_count += 1
        blk_addr, size_byte, = int(row["lba"]), int(row["size"])
        size_block = size_byte//blk_size_byte
        for cur_lba in range(blk_addr, blk_addr + size_block):
            blk_addr_set.add(cur_lba)
        if line_count % 1000000 == 0:
            print("unique lba tracking {}% completed".format(100*line_count/total_line))

    return blk_addr_set