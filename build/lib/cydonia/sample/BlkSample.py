from copy import deepcopy
from pathlib import Path
from gpg import Data
from pandas import DataFrame, read_csv 
from numpy import inf, ndarray, zeros, mean, std 


def get_first_block_req(
        per_blk_access_stat_dict: dict,
        filter_index: int = -1
) -> dict:
    """Get the information of the first block request according to the per block statistics. 

    Args:
        per_blk_access_stat_dict: Dictionary of per block access statistics. 
        filter_index: Index value to be ignored. 
    
    Returns:
        first_block_req_dict: Dictionary of information on the first block request. 
    """
    min_index = inf
    blk_addr_arr = []
    op, iat = '', -1
    for blk_addr in per_blk_access_stat_dict:
        if filter_index == per_blk_access_stat_dict[blk_addr]['i']:
            continue 
        if per_blk_access_stat_dict[blk_addr]['i'] < min_index:
            min_index = per_blk_access_stat_dict[blk_addr]['i']
            blk_addr_arr = [blk_addr]
            op = per_blk_access_stat_dict[blk_addr]["op0"]
            iat = per_blk_access_stat_dict[blk_addr]["iat0"]
        elif per_blk_access_stat_dict[blk_addr]['i'] == min_index:
            blk_addr_arr.append(blk_addr)
            assert op == per_blk_access_stat_dict[blk_addr]["op0"], \
                "The block addresses with same index should have the same op. {} vs {}".format(op, per_blk_access_stat_dict[blk_addr]["op0"])
            assert iat == per_blk_access_stat_dict[blk_addr]["iat0"], \
                "The block addresses with same index should have the same iat. {} vs {}".format(iat, per_blk_access_stat_dict[blk_addr]["iat0"])
            
    return {
        'blk_addr_arr': blk_addr_arr,
        'op': op,
        'min_index': min_index,
        'iat': iat 
    }


def get_blk_addr_arr(
        block_addr: int,
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
    region_index = block_addr >> num_lower_order_bits_ignored
    num_block_in_region = 2**num_lower_order_bits_ignored
    block_addr_arr = zeros(num_block_in_region, dtype=int)
    for block_index in range(num_block_in_region):
        block_addr_arr[block_index] = (region_index << num_lower_order_bits_ignored) + block_index
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

    full_mean_read_size = full_stat_dict["total_read_size"]/full_stat_dict["read_count"]
    sample_mean_read_size = sample_stat_dict["total_read_size"]/sample_stat_dict["read_count"]
    percent_error_dict["mean_read_size"] = 100.0*(full_mean_read_size - sample_mean_read_size)/full_mean_read_size

    full_mean_write_size = full_stat_dict["total_write_size"]/full_stat_dict["write_count"]
    sample_mean_write_size = sample_stat_dict["total_write_size"]/sample_stat_dict["write_count"]
    percent_error_dict["mean_write_size"] = 100.0*(full_mean_write_size - sample_mean_write_size)/full_mean_write_size

    full_mean_read_iat = full_stat_dict["total_read_iat"]/full_stat_dict["read_count"]
    sample_mean_read_iat = sample_stat_dict["total_read_iat"]/sample_stat_dict["read_count"]
    percent_error_dict["mean_read_iat"] = 100.0*(full_mean_read_iat - sample_mean_read_iat)/full_mean_read_iat

    full_mean_write_iat = full_stat_dict["total_write_iat"]/full_stat_dict["write_count"]
    sample_mean_write_iat = sample_stat_dict["total_write_iat"]/sample_stat_dict["write_count"]
    percent_error_dict["mean_write_iat"] = 100.0*(full_mean_write_iat - sample_mean_write_iat)/full_mean_write_iat

    full_write_ratio = full_stat_dict["write_count"]/(full_stat_dict["read_count"] + full_stat_dict["write_count"])
    sample_write_ratio = sample_stat_dict["write_count"]/(sample_stat_dict["read_count"] + sample_stat_dict["write_count"])
    percent_error_dict["write_ratio"] = 100.0 * (full_write_ratio - sample_write_ratio)/full_write_ratio

    mean_err = mean(list([abs(_) for _ in percent_error_dict.values()]))
    std_dev = std(list([abs(_) for _ in percent_error_dict.values()]))

    percent_error_dict["mean"] = mean_err 
    percent_error_dict["std"] = std_dev
    return percent_error_dict
    

def eval_all_blk(
        full_workload_stat_dict: dict, 
        workload_stat_dict: dict,
        per_blk_access_stat_dict: dict, 
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512
) -> DataFrame:
    """Evaluate the percent error when removing each region from the trace. 
    """
    blk_addr_list = list(per_blk_access_stat_dict.keys())
    num_blk_evaluated = 0 
    new_workload_stat_arr = []
    track_computed_region_dict = {}
    
    for blk_addr in blk_addr_list:

        print("{}% completed.".format(int(100*num_blk_evaluated/len(blk_addr_list))))

        num_blk_evaluated += 1 
        region_index = blk_addr >> num_lower_order_bits_ignored
        if region_index in track_computed_region_dict:
            if num_blk_evaluated % 10000 == 0:
                print("{}% completed.".format(int(100*num_blk_evaluated/len(blk_addr_list))))
            continue 
        track_computed_region_dict[region_index] = 1 

        cur_workload_stat_dict = deepcopy(workload_stat_dict)
        cur_per_blk_access_stat_dict = deepcopy(per_blk_access_stat_dict)
        region_blk_addr_arr = get_blk_addr_arr(blk_addr, num_lower_order_bits_ignored)

        for region_blk_addr in region_blk_addr_arr:
            cur_workload_stat_dict = remove_block(cur_workload_stat_dict, cur_per_blk_access_stat_dict, region_blk_addr, blk_size_byte=blk_size_byte)
        
        percent_error_dict = get_percent_error_dict(full_workload_stat_dict, cur_workload_stat_dict)
        percent_error_dict["region"] = region_index
        new_workload_stat_arr.append(percent_error_dict)

        if num_blk_evaluated % 10000 == 0:
            print("{}% completed.".format(int(100*num_blk_evaluated/len(blk_addr_list))))

    return DataFrame(new_workload_stat_arr)


def remove_block(
        workload_stat_dict: dict,
        per_blk_access_stat_dict: dict, 
        blk_addr: int,
        blk_size_byte: int = 512
) -> dict:
    """Remove a block from a block trace and return the new workload features.

    Args:
        blk_trace_df: DataFrame containing the block trace. 
        blk_addr: Block address to remove. 
        blk_size_byte: Size of a block in bytes. (Default: 512)
    
    Returns:
        new_workload_feature_dict: Dictionary of workload features after removal of the block.
    """
    # blk access statistics 
    blk_access_stat_dict = per_blk_access_stat_dict[blk_addr]
    new_sub_remove_stat_dict = get_new_sub_remove_stat_dict(blk_access_stat_dict, blk_addr-1 in per_blk_access_stat_dict, blk_addr+1 in per_blk_access_stat_dict)
    
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

    # adjust total IAT if the first request of the block request is being changed 
    first_blk_req_dict = get_first_block_req(per_blk_access_stat_dict)
    if blk_addr in first_blk_req_dict["blk_addr_arr"] and len(first_blk_req_dict["blk_addr_arr"]) == 1:
        second_blk_req_dict = get_first_block_req(per_blk_access_stat_dict, filter_index=first_blk_req_dict["min_index"])
        if second_blk_req_dict["op"] == 'r':
            new_workload_stat_dict["total_read_iat"] -= (second_blk_req_dict["iat"] - first_blk_req_dict["iat"])
        elif second_blk_req_dict["op"] == 'w':
            new_workload_stat_dict["total_write_iat"] -= (second_blk_req_dict["iat"] - first_blk_req_dict["iat"])
        else:
            raise ValueError("Unrecognized request type {}.".format(second_blk_req_dict["op"]))

    new_workload_stat_dict["write_ratio"] = new_workload_stat_dict["write_count"]/(new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"])
    new_workload_stat_dict["mean_read_size"] = new_workload_stat_dict["total_read_size"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_size"] = new_workload_stat_dict["total_write_size"]/new_workload_stat_dict["write_count"]
    new_workload_stat_dict["mean_read_iat"] = new_workload_stat_dict["total_read_iat"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_iat"] = new_workload_stat_dict["total_write_iat"]/new_workload_stat_dict["write_count"]

    left_accessed, right_accessed = blk_addr-1 in per_blk_access_stat_dict, blk_addr+1 in per_blk_access_stat_dict
    if left_accessed:
        per_blk_access_stat_dict[blk_addr-1]["r_right_count"] += per_blk_access_stat_dict[blk_addr-1]["r_mid_count"]
        per_blk_access_stat_dict[blk_addr-1]["w_right_count"] += per_blk_access_stat_dict[blk_addr-1]["w_mid_count"]
        per_blk_access_stat_dict[blk_addr-1]["r_right_iat_sum"] += per_blk_access_stat_dict[blk_addr-1]["r_mid_iat_sum"]
        per_blk_access_stat_dict[blk_addr-1]["w_right_iat_sum"] += per_blk_access_stat_dict[blk_addr-1]["w_mid_iat_sum"]
        per_blk_access_stat_dict[blk_addr-1]["r_mid_count"], per_blk_access_stat_dict[blk_addr-1]["w_mid_count"] = 0, 0 
        per_blk_access_stat_dict[blk_addr-1]["r_mid_iat_sum"], per_blk_access_stat_dict[blk_addr-1]["w_mid_iat_sum"] = 0, 0 

        # all request of the left block where it was the left most block now tuns into a solo access 
        per_blk_access_stat_dict[blk_addr-1]["r_solo_count"] += per_blk_access_stat_dict[blk_addr-1]["r_left_count"]
        per_blk_access_stat_dict[blk_addr-1]["w_solo_count"] += per_blk_access_stat_dict[blk_addr-1]["w_left_count"]
        per_blk_access_stat_dict[blk_addr-1]["r_solo_iat_sum"] += per_blk_access_stat_dict[blk_addr-1]["r_left_iat_sum"]
        per_blk_access_stat_dict[blk_addr-1]["w_solo_iat_sum"] += per_blk_access_stat_dict[blk_addr-1]["w_left_iat_sum"]
        per_blk_access_stat_dict[blk_addr-1]["r_left_count"], per_blk_access_stat_dict[blk_addr-1]["w_left_count"] = 0, 0 
        per_blk_access_stat_dict[blk_addr-1]["r_left_iat_sum"], per_blk_access_stat_dict[blk_addr-1]["w_left_iat_sum"] = 0, 0 
    
    if right_accessed:
        # all requests of the right block where it used to be the mid block is now turns into the left most block
        per_blk_access_stat_dict[blk_addr+1]["r_left_count"] += per_blk_access_stat_dict[blk_addr+1]["r_mid_count"]
        per_blk_access_stat_dict[blk_addr+1]["w_left_count"] += per_blk_access_stat_dict[blk_addr+1]["w_mid_count"]
        per_blk_access_stat_dict[blk_addr+1]["r_left_iat_sum"] += per_blk_access_stat_dict[blk_addr+1]["r_mid_iat_sum"]
        per_blk_access_stat_dict[blk_addr+1]["w_left_iat_sum"] += per_blk_access_stat_dict[blk_addr+1]["w_mid_iat_sum"]
        per_blk_access_stat_dict[blk_addr+1]["r_mid_count"], per_blk_access_stat_dict[blk_addr+1]["w_mid_count"] = 0, 0 
        per_blk_access_stat_dict[blk_addr+1]["r_mid_iat_sum"], per_blk_access_stat_dict[blk_addr+1]["w_mid_iat_sum"] = 0, 0 

        # all request of the right block where it was the right most block now tuns into a solo access 
        per_blk_access_stat_dict[blk_addr+1]["r_solo_count"] += per_blk_access_stat_dict[blk_addr+1]["r_right_count"]
        per_blk_access_stat_dict[blk_addr+1]["w_solo_count"] += per_blk_access_stat_dict[blk_addr+1]["w_right_count"]
        per_blk_access_stat_dict[blk_addr+1]["r_solo_iat_sum"] += per_blk_access_stat_dict[blk_addr+1]["r_right_iat_sum"]
        per_blk_access_stat_dict[blk_addr+1]["w_solo_iat_sum"] += per_blk_access_stat_dict[blk_addr+1]["w_right_iat_sum"]
        per_blk_access_stat_dict[blk_addr+1]["r_right_count"], per_blk_access_stat_dict[blk_addr+1]["w_right_count"] = 0, 0 
        per_blk_access_stat_dict[blk_addr+1]["r_right_iat_sum"], per_blk_access_stat_dict[blk_addr+1]["w_right_iat_sum"] = 0, 0 
    
    per_blk_access_stat_dict.pop(blk_addr)

    return new_workload_stat_dict


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






def add_block(
        workload_stat_dict: dict,
        per_blk_access_stat_dict: dict,
        sample_block_addr_dict: dict, 
        blk_addr: int,
        blk_size_byte: int = 512 
) -> dict:
    left_sampled = blk_addr - 1 in sample_block_addr_dict
    right_sampled = blk_addr + 1 in sample_block_addr_dict
    blk_access_dict = per_blk_access_stat_dict[blk_addr]

    print(blk_access_dict)

    new_workload_stat_dict = deepcopy(workload_stat_dict)

    new_workload_stat_dict["read_count"] += blk_access_dict["r_solo_count"]
    new_workload_stat_dict["total_read_iat"] += blk_access_dict["r_solo_iat_sum"]
    new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_solo_count"] * blk_size_byte)

    new_workload_stat_dict["write_count"] += blk_access_dict["w_solo_count"]
    new_workload_stat_dict["total_write_iat"] += blk_access_dict["w_solo_iat_sum"]
    new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_solo_count"] * blk_size_byte)

    if left_sampled and right_sampled:
        new_workload_stat_dict["read_count"] -= blk_access_dict["r_mid_count"]
        new_workload_stat_dict["write_count"] -= blk_access_dict["w_mid_count"]

        new_workload_stat_dict["total_read_iat"] -= blk_access_dict["r_mid_iat_sum"]
        new_workload_stat_dict["total_write_iat"] -= blk_access_dict["w_mid_iat_sum"]

        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_mid_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_mid_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_size"] += ((blk_access_dict["r_left_count"]+blk_access_dict["r_right_count"]) * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += ((blk_access_dict["w_left_count"]+blk_access_dict["w_right_count"]) * blk_size_byte)
    elif not left_sampled and right_sampled:
        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_mid_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_mid_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_left_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_left_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_right_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_right_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_iat"] += blk_access_dict["r_right_iat_sum"]
        new_workload_stat_dict["total_write_iat"] += blk_access_dict["w_right_iat_sum"]

        new_workload_stat_dict["read_count"] += blk_access_dict["r_right_count"]
        new_workload_stat_dict["write_count"] += blk_access_dict["w_right_count"]
    elif left_sampled and not right_sampled:
        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_mid_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_mid_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_right_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_right_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_size"] += (blk_access_dict["r_left_count"] * blk_size_byte)
        new_workload_stat_dict["total_write_size"] += (blk_access_dict["w_left_count"] * blk_size_byte)

        new_workload_stat_dict["total_read_iat"] += blk_access_dict["r_left_iat_sum"]
        new_workload_stat_dict["total_write_iat"] += blk_access_dict["w_left_iat_sum"]

        new_workload_stat_dict["read_count"] += blk_access_dict["r_left_count"]
        new_workload_stat_dict["write_count"] += blk_access_dict["w_left_count"]
    else:
        new_workload_stat_dict["read_count"] += (blk_access_dict["r_left_count"]+blk_access_dict["r_right_count"])
        new_workload_stat_dict["write_count"] += (blk_access_dict["w_left_count"]+blk_access_dict["w_right_count"])  

        new_workload_stat_dict["total_read_size"] += (blk_size_byte * (blk_access_dict["r_left_count"]+blk_access_dict["r_right_count"]))
        new_workload_stat_dict["total_write_size"] += (blk_size_byte * (blk_access_dict["w_left_count"]+blk_access_dict["w_right_count"]))

        new_workload_stat_dict["total_read_iat"] += (blk_access_dict["r_left_iat_sum"]+blk_access_dict["r_right_iat_sum"])
        new_workload_stat_dict["total_write_iat"] += (blk_access_dict["w_left_iat_sum"]+blk_access_dict["w_right_iat_sum"])

    new_workload_stat_dict["write_ratio"] = new_workload_stat_dict["write_count"]/(new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"])
    new_workload_stat_dict["mean_read_size"] = new_workload_stat_dict["total_read_size"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_size"] = new_workload_stat_dict["total_write_size"]/new_workload_stat_dict["write_count"]
    new_workload_stat_dict["mean_read_iat"] = new_workload_stat_dict["total_read_iat"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_iat"] = new_workload_stat_dict["total_write_iat"]/new_workload_stat_dict["write_count"]
    return new_workload_stat_dict


def get_workload_stat_dict(df: DataFrame) -> dict:
    """Get the statistics from a DataFram with the block trace.
    
    Args:
        df: DataFrame with the block trace. 
    
    Returns:
        stat_dict: Dictionary of overall stat from block trace df. 
    """
    stat_dict = {}
    stat_dict["read_count"] = len(df[df['op']=='r'])
    stat_dict["write_count"] = len(df[df['op']=='w'])
    stat_dict["total_read_size"] = df[df['op']=='r']['size'].sum()
    stat_dict["total_write_size"] =  df[df['op']=='w']['size'].sum()
    stat_dict["total_read_iat"] = df[df['op']=='r']['iat'].sum()
    stat_dict["total_write_iat"] = df[df['op']=='w']['iat'].sum()
    stat_dict["write_ratio"] = stat_dict["write_count"]/(stat_dict["read_count"] + stat_dict["write_count"])
    stat_dict["mean_read_size"] = stat_dict["total_read_size"]/stat_dict["read_count"]
    stat_dict["mean_write_size"] = stat_dict["total_write_size"]/stat_dict["write_count"]
    stat_dict["mean_read_iat"] = stat_dict["total_read_iat"]/stat_dict["read_count"]
    stat_dict["mean_write_iat"] = stat_dict["total_write_iat"]/stat_dict["write_count"]
    return stat_dict 



class BlkSample:
    def __init__(
            self,
            block_trace_path: Path,
            sample_trace_path: Path 
    ) -> None:
        self._block_df = self.load_block_trace(block_trace_path)
        self._sample_df = self.load_block_trace(sample_trace_path)
        self._per_block_access_stat_dict = self.get_per_block_access_stat_dict(self._sample_df)


    @staticmethod
    def get_per_block_access_stat_dict(
            df: DataFrame,
            block_size_byte: int = 512 
    ) -> dict:
        """Get the dictionary of access statistics. 

        Args:
            df: DataFrame of block trace. 
            block_size_byte: Size of block in byte. 
        
        Returns:
            access_stat_dict: Get access statistics from a block trace. 
        """
        access_stat_dict = {}
        req_count_tracker = 0 
        for _, row in df.iterrows():
            req_count_tracker += 1
            block_addr, size_byte, op, ts = int(row["lba"]), int(row["size"]), row["op"], int(row["ts"])
            size_block = size_byte//block_size_byte
            assert size_byte % 512 == 0 
            
            try:
                cur_iat = int(row["iat"])
            except ValueError:
                cur_iat = 0

            if size_block == 1:
                if block_addr not in access_stat_dict:
                    access_stat_dict[block_addr] = dict(BlkSample.init_access_stat_dict(ts, cur_iat, op, req_count_tracker))

                access_stat_dict[block_addr]["{}_solo_count".format(op)] += 1 
                access_stat_dict[block_addr]["{}_solo_iat_sum".format(op)] += cur_iat
                continue 

            start_lba = block_addr
            end_lba = start_lba + size_block
            for cur_lba in range(start_lba, end_lba):
                if cur_lba not in access_stat_dict:
                    access_stat_dict[cur_lba] = dict(BlkSample.init_access_stat_dict(ts, cur_iat, op, req_count_tracker))

                if cur_lba == start_lba:
                    access_stat_dict[cur_lba]["{}_left_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_left_iat_sum".format(op)] += cur_iat
                elif cur_lba == start_lba+size_block-1:
                    access_stat_dict[cur_lba]["{}_right_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_right_iat_sum".format(op)] += cur_iat
                else:
                    access_stat_dict[cur_lba]["{}_mid_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_mid_iat_sum".format(op)] += cur_iat
        return access_stat_dict


    @staticmethod
    def init_access_stat_dict(
            ts0: int, 
            iat0: int, 
            op0: str, 
            i: int 
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
            "w_mid_iat_sum": 0,

            "ts0": ts0,
            "iat0": iat0,
            "op0": op0,
            "i": i 
        }
    

    @staticmethod
    def load_block_trace(
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