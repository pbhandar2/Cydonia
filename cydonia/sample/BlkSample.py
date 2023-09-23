
from pathlib import Path
from copy import deepcopy
from time import perf_counter
from queue import PriorityQueue
from urllib import request
from pandas import DataFrame, read_csv 
from numpy import inf, ndarray, zeros, mean, std 


class BlkReq:
    def __init__(
            self, 
            blk_addr: int, 
            size: int, 
            op: str, 
            i: int, 
            iat: int 
    ) -> None:
        self.addr = blk_addr
        self.size = size
        self.op = op 
        self.index = i 
        self.iat = iat 
    

    def contains_blk_addr(self, addr: int):
        return addr >= self.addr and addr < self.addr + self.size 


    def is_solo_req(self, addr: int):
        return self.contains_blk_addr(addr) and self.size == 1


    def __lt__(self, other):
        # overload < operator so that we can use it with a PriorityQueue
        return self.index < other.index



def blk_unsample(
        sample_df: DataFrame,
        workload_stat_dict: dict,
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512
) -> DataFrame:
    """Reduce sample workload feature error by unsampling (removing blocks).

    Args:
        sample_df: DataFrame of sample block trace. 
        workload_stat_dict: Dictionary of statistics of full workload features. 
        num_lower_order_bits_ignored: Number of lower order bits ignored. 
        blk_size_byte: Size of block in byte. 
    """
    per_blk_access_stat_dict, first_req_order_queue = get_access_stat_and_first_req_queue(sample_df)

    # load an ordered array of first block request of each block request 
    first_blk_req_arr = []
    for index in range(first_req_order_queue.qsize()):
        _, item = first_req_order_queue.get()
        first_blk_req_arr.append(item)

    # start tracking the current first and second block req 
    first_req_index_tracker = 0 
    sample_workload_stat_dict = get_workload_stat_dict(sample_df)
    remove_error_df = get_err_df_on_remove(per_blk_access_stat_dict, 
                                            first_blk_req_arr, 
                                            sample_workload_stat_dict, 
                                            workload_stat_dict,
                                            num_lower_order_bits_ignored=num_lower_order_bits_ignored)

    percent_err_dict_arr = []
    new_per_blk_access_stat_dict = {}
    new_workload_stat_dict = deepcopy(sample_workload_stat_dict)
    while len(remove_error_df):
        best_row = remove_error_df.sort_values(by=["mean"]).iloc[0]
        region_addr = best_row["region"]

        new_workload_stat_dict, new_first_req_index_tracker = remove_region(region_addr, 
                                                                                new_workload_stat_dict, 
                                                                                per_blk_access_stat_dict, 
                                                                                new_per_blk_access_stat_dict,
                                                                                first_blk_req_arr[first_req_index_tracker:],
                                                                                num_lower_order_bits_ignored)
        first_req_index_tracker += new_first_req_index_tracker
        # region_blk_addr_arr = get_blk_addr_arr(int(region_addr), num_lower_order_bits_ignored)
        # for blk_addr in region_blk_addr_arr:
        #     if blk_addr not in per_blk_access_stat_dict:
        #         continue 

        #     if blk_addr in new_per_blk_access_stat_dict:
        #         cur_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr]
        #     else:
        #         cur_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr]

        #     right_blk_access_stat_dict = {}
        #     if blk_addr+1 in new_per_blk_access_stat_dict:
        #         right_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr+1]
        #     elif blk_addr+1 in per_blk_access_stat_dict:
        #         right_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr+1]

        #     left_blk_access_stat_dict = {}
        #     if blk_addr-1 in new_per_blk_access_stat_dict:
        #         left_blk_access_stat_dict = new_per_blk_access_stat_dict[blk_addr-1]
        #     elif blk_addr-1 in per_blk_access_stat_dict:
        #         left_blk_access_stat_dict = per_blk_access_stat_dict[blk_addr-1]
            
        #     return_tuple = remove_blk(blk_addr, 
        #                                 new_workload_stat_dict, 
        #                                 cur_blk_access_stat_dict,
        #                                 left_blk_access_stat_dict,
        #                                 right_blk_access_stat_dict,
        #                                 first_blk_req,
        #                                 second_blk_req,
        #                                 blk_size_byte=blk_size_byte)
            
        #     new_workload_stat_dict, new_left_blk_access_stat_dict, new_right_blk_access_stat_dict = return_tuple

        #     new_per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
        #     new_per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict 

        #     if first_blk_req.is_solo_req(blk_addr):
        #         first_req_index_tracker += 1
        #         first_blk_req = first_blk_req_arr[first_req_index_tracker]
        #         second_blk_req = first_blk_req_arr[first_req_index_tracker+1]

        #     per_blk_access_stat_dict.pop(blk_addr)
        #     if new_left_blk_access_stat_dict:
        #         per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
            
        #     if new_right_blk_access_stat_dict:
        #         per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict

        percent_error_dict = get_percent_error_dict(workload_stat_dict, new_workload_stat_dict)
        percent_error_dict["region"] = region_addr
        percent_err_dict_arr.append(percent_error_dict)
        remove_error_df = get_err_df_on_remove(per_blk_access_stat_dict, 
                                                first_blk_req_arr[first_req_index_tracker:], 
                                                new_workload_stat_dict, 
                                                workload_stat_dict,
                                                num_lower_order_bits_ignored=num_lower_order_bits_ignored)
    return DataFrame(percent_err_dict_arr)


def remove_region(
        region_addr: int,
        workload_stat_dict: dict,
        per_blk_access_stat_dict: dict,
        new_per_blk_access_stat_dict: dict,
        first_blk_req_arr: list,
        num_lower_order_bits_ignored: int,
        blk_size_byte: int = 512
) -> tuple:
    # start tracking the current first and second block req 
    first_req_index_tracker = 0 
    first_blk_req = first_blk_req_arr[first_req_index_tracker]
    second_blk_req = first_blk_req_arr[first_req_index_tracker+1]
    if first_req_index_tracker + 1 >= len(first_blk_req_arr):
        second_blk_req = BlkReq(-1, -1, '', -1, 0)
    else:
        second_blk_req = first_blk_req_arr[first_req_index_tracker+1]

    new_workload_stat_dict = deepcopy(workload_stat_dict)
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
                                    first_blk_req,
                                    second_blk_req,
                                    blk_size_byte=blk_size_byte)
        
        new_workload_stat_dict, new_left_blk_access_stat_dict, new_right_blk_access_stat_dict = return_tuple
        new_per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
        new_per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict 

        if first_blk_req.is_solo_req(blk_addr):
            first_req_index_tracker += 1
            first_blk_req = first_blk_req_arr[first_req_index_tracker]
            if first_req_index_tracker + 1 >= len(first_blk_req_arr):
                second_blk_req = BlkReq(-1, -1, '', -1, 0)
            else:
                second_blk_req = first_blk_req_arr[first_req_index_tracker+1]
                
        per_blk_access_stat_dict.pop(blk_addr)
        if new_left_blk_access_stat_dict:
            per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
        
        if new_right_blk_access_stat_dict:
            per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict
    return new_workload_stat_dict, first_req_index_tracker


def remove_blk(
        blk_addr: int, 
        workload_stat_dict: dict, 
        blk_access_stat_dict: dict,
        left_blk_access_stat_dict: dict,
        right_blk_access_stat_dict: dict,
        first_blk_req: BlkReq,
        second_blk_req: BlkReq, 
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

    # adjust total IAT if the first request of the block request is being changed 
    # first_blk_req_dict = get_first_block_req(per_blk_access_stat_dict)
    if first_blk_req.is_solo_req(blk_addr):
        # second_blk_req_dict = get_first_block_req(per_blk_access_stat_dict, filter_index=first_blk_req_dict["min_index"])
        if second_blk_req.op == 'r':
            new_workload_stat_dict["total_read_iat"] -= (second_blk_req.iat - first_blk_req.iat)
        elif second_blk_req.op == 'w':
            new_workload_stat_dict["total_write_iat"] -= (second_blk_req.iat - first_blk_req.iat)
        else:
            raise ValueError("Unrecognized request type {}.".format(second_blk_req.op))


    new_workload_stat_dict["write_ratio"] = new_workload_stat_dict["write_count"]/(new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"])
    new_workload_stat_dict["mean_read_size"] = new_workload_stat_dict["total_read_size"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_size"] = new_workload_stat_dict["total_write_size"]/new_workload_stat_dict["write_count"]
    new_workload_stat_dict["mean_read_iat"] = new_workload_stat_dict["total_read_iat"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_iat"] = new_workload_stat_dict["total_write_iat"]/new_workload_stat_dict["write_count"]

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


def get_err_df_on_remove(
        per_blk_access_stat_dict: dict, 
        first_blk_req_arr: list, 
        sample_workload_stat_dict: dict, 
        workload_stat_dict: dict,
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512
) -> DataFrame:
    """Get DataFrame of error values when removing blocks. 

    Args:
        sample_df: DataFrame of sample block trace. 
    """
    region_addr_arr = list(set([block_addr >> num_lower_order_bits_ignored for block_addr in per_blk_access_stat_dict]))
    num_region = len(region_addr_arr)

    first_req_index_tracker = 0 
    first_blk_req = first_blk_req_arr[first_req_index_tracker]
    second_blk_req = first_blk_req_arr[first_req_index_tracker+1]

    error_dict_arr = []
    for index, region_addr in enumerate(region_addr_arr):
        region_blk_addr_arr = get_blk_addr_arr(region_addr, num_lower_order_bits_ignored)

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
                                        first_blk_req,
                                        second_blk_req,
                                        blk_size_byte=blk_size_byte)
            
            new_workload_stat_dict, new_left_blk_access_stat_dict, new_right_blk_access_stat_dict = return_tuple

            new_per_blk_access_stat_dict[blk_addr-1] = new_left_blk_access_stat_dict
            new_per_blk_access_stat_dict[blk_addr+1] = new_right_blk_access_stat_dict 

            if first_blk_req.is_solo_req(blk_addr):
                first_req_index_tracker += 1
                first_blk_req = first_blk_req_arr[first_req_index_tracker]
                second_blk_req = first_blk_req_arr[first_req_index_tracker+1]
        
        percent_error_dict = get_percent_error_dict(workload_stat_dict, new_workload_stat_dict)
        percent_error_dict["region"] = region_addr
        error_dict_arr.append(percent_error_dict)
        # if index % 10000:
        #     print("{}/{} completed".format(index, num_region))

    return DataFrame(error_dict_arr)



def get_access_stat_and_first_req_queue(
        df: DataFrame,
        block_size_byte: int = 512 
) -> tuple:
    """Get the dictionary of access statistics. 

    Args:
        df: DataFrame of block trace. 
        block_size_byte: Size of block in byte. 
    
    Returns:
        access_stat_dict: Get access statistics from a block trace. 
    """
    access_stat_dict = {}
    first_req_queue = PriorityQueue()
    req_count_tracker = 0 
    blk_count_tracker = 0 
    for _, row in df.iterrows():
        req_count_tracker += 1
        blk_addr, size_byte, op, ts = int(row["lba"]), int(row["size"]), row["op"], int(row["ts"])
        size_block = size_byte//block_size_byte
        assert size_byte % 512 == 0 
        
        try:
            cur_iat = int(row["iat"])
        except ValueError:
            cur_iat = 0

        if size_block == 1:
            if blk_addr not in access_stat_dict:
                blk_count_tracker += 1
                access_stat_dict[blk_addr] = dict(BlkSample.init_access_stat_dict(ts, cur_iat, op, req_count_tracker))
                first_req_queue.put((blk_count_tracker, BlkReq(blk_addr, size_block, op, req_count_tracker, cur_iat)))
                

            access_stat_dict[blk_addr]["{}_solo_count".format(op)] += 1 
            access_stat_dict[blk_addr]["{}_solo_iat_sum".format(op)] += cur_iat
            continue 

        start_lba = blk_addr
        end_lba = start_lba + size_block
        for cur_lba in range(start_lba, end_lba):
            if cur_lba not in access_stat_dict:
                access_stat_dict[cur_lba] = dict(BlkSample.init_access_stat_dict(ts, cur_iat, op, req_count_tracker))
                first_req_queue.put((blk_count_tracker, BlkReq(blk_addr, size_block, op, blk_count_tracker, cur_iat)))

            if cur_lba == start_lba:
                access_stat_dict[cur_lba]["{}_left_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_left_iat_sum".format(op)] += cur_iat
            elif cur_lba == start_lba+size_block-1:
                access_stat_dict[cur_lba]["{}_right_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_right_iat_sum".format(op)] += cur_iat
            else:
                access_stat_dict[cur_lba]["{}_mid_count".format(op)] += 1 
                access_stat_dict[cur_lba]["{}_mid_iat_sum".format(op)] += cur_iat
    return access_stat_dict, first_req_queue 



def get_remove_error_df(
        sample_df: DataFrame, 
        full_workload_stat_dict: dict,
        num_lower_order_bits_ignored: int = 0,
        blk_size_byte: int = 512
) -> DataFrame:
    start_time = perf_counter()
    num_blk_evaluated = 0 
    new_workload_stat_arr = []
    track_computed_region_dict = {}
    per_blk_access_stat_dict, queue = get_access_stat_and_first_req_queue(sample_df)
    blk_addr_list = list(per_blk_access_stat_dict.keys())
    blk_addr_list_len = len(blk_addr_list)
    workload_stat_dict = get_workload_stat_dict(sample_df)
    end_time = perf_counter()
    #print("Preprocessing done in {} minutes".format(float(end_time - start_time)/60))

    _, first_blk_req = queue.get()
    _, second_blk_req = queue.get()
    for blk_addr in blk_addr_list:
        pre_process_start_time = perf_counter()

        region_tracker_start_time = perf_counter()
        num_blk_evaluated += 1 
        region_index = blk_addr >> num_lower_order_bits_ignored
        if region_index in track_computed_region_dict:
            if num_blk_evaluated % 5 == 0:
                time_len = perf_counter() - start_time 
                print("{}/{} completed in {} minutes.".format(num_blk_evaluated, blk_addr_list_len, float(time_len)/60))
            continue 
        track_computed_region_dict[region_index] = 1 
        region_tracker_end_time = perf_counter()
        #print("region tracker {} in {} seconds.".format(region_index, region_tracker_end_time - region_tracker_start_time))

        data_copy_start_time = perf_counter()
        cur_workload_stat_dict = deepcopy(workload_stat_dict)
        #cur_per_blk_access_stat_dict = deepcopy(per_blk_access_stat_dict)
        data_copy_end_time = perf_counter()
        #print("copy time {} in {} seconds".format(region_index, data_copy_end_time - data_copy_start_time))

        get_blk_addr_start_time = perf_counter()
        region_blk_addr_arr = get_blk_addr_arr(blk_addr, num_lower_order_bits_ignored)
        get_blk_addr_end_time = perf_counter()
        #print("get block addr {} in {} seconds.".format(region_index, get_blk_addr_end_time - get_blk_addr_start_time))
        pre_process_end_time = perf_counter()
        #print("Preprocess {} in {} seconds.".format(region_index, pre_process_end_time - pre_process_start_time))

        remove_start_time = perf_counter()
        if num_lower_order_bits_ignored == 0:
            cur_workload_stat_dict = remove_block(cur_workload_stat_dict, 
                                                    per_blk_access_stat_dict, 
                                                    region_blk_addr_arr[0], 
                                                    first_blk_req, 
                                                    second_blk_req, 
                                                    blk_size_byte=blk_size_byte,
                                                    update_stat_dict=False)
            if first_blk_req.is_solo_req(region_blk_addr_arr[0]):
                first_blk_req = second_blk_req
                _, second_blk_req = queue.get()
        else:
            cur_per_blk_access_stat_dict = deepcopy(per_blk_access_stat_dict)
            for region_blk_addr in region_blk_addr_arr:
                if region_blk_addr not in per_blk_access_stat_dict:
                    continue 
                cur_workload_stat_dict = remove_block(cur_workload_stat_dict, 
                                                        cur_per_blk_access_stat_dict, 
                                                        region_blk_addr, 
                                                        first_blk_req, 
                                                        second_blk_req, 
                                                        blk_size_byte=blk_size_byte,
                                                        update_stat_dict=True)
                if first_blk_req.is_solo_req(region_blk_addr):
                    first_blk_req = second_blk_req
                    _, second_blk_req = queue.get()
        remove_end_time = perf_counter()
        #print("Removed {} in {} seconds.".format(region_index, remove_end_time - remove_start_time))
        
        post_process_start_time = perf_counter()
        percent_error_dict = get_percent_error_dict(full_workload_stat_dict, cur_workload_stat_dict)
        percent_error_dict["region"] = region_index
        new_workload_stat_arr.append(percent_error_dict)
        if num_blk_evaluated % 5 == 0:
            time_len = perf_counter() - start_time 
            print("{}/{} completed in {} minutes.".format(num_blk_evaluated, blk_addr_list_len, float(time_len)/60))
        post_process_end_time = perf_counter()
        #print("Postprocess {} in {} seconds.".format(region_index, post_process_end_time - post_process_start_time))

    return DataFrame(new_workload_stat_arr)




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
        #print(region_addr)
        #ßprint(type(region_addr))
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

    start_time = perf_counter()
    num_blk_evaluated = 0 
    new_workload_stat_arr = []
    track_computed_region_dict = {}
    blk_addr_list = list(per_blk_access_stat_dict.keys())

    queue = PriorityQueue()
    for blk_addr in per_blk_access_stat_dict:
        queue.put(per_blk_access_stat_dict[blk_addr]["i"], per_blk_access_stat_dict[blk_addr])

    _, first_blk_req_dict = queue.get()
    _, second_blk_req_dict = queue.get()
    
    for blk_addr in blk_addr_list:
        num_blk_evaluated += 1 

        region_index = blk_addr >> num_lower_order_bits_ignored
        if region_index in track_computed_region_dict:
            if num_blk_evaluated % 5 == 0:
                time_len = perf_counter() - start_time 
                print("{}, {}% completed in {}.".format(num_blk_evaluated, int(100*num_blk_evaluated/len(blk_addr_list)), time_len))
            continue 
        track_computed_region_dict[region_index] = 1 

        cur_workload_stat_dict = deepcopy(workload_stat_dict)
        cur_per_blk_access_stat_dict = deepcopy(per_blk_access_stat_dict)
        region_blk_addr_arr = get_blk_addr_arr(blk_addr, num_lower_order_bits_ignored)
        for region_blk_addr in region_blk_addr_arr:
            cur_workload_stat_dict = remove_block(cur_workload_stat_dict, 
                                                    cur_per_blk_access_stat_dict, 
                                                    region_blk_addr, 
                                                    first_blk_req_dict, 
                                                    second_blk_req_dict, 
                                                    blk_size_byte=blk_size_byte)
            
            if blk_addr in first_blk_req_dict["blk_addr_arr"] and len(first_blk_req_dict["blk_addr_arr"]) == 1:
                first_blk_req_dict = second_blk_req_dict
                _, second_blk_req_dict = queue.get()
        
        percent_error_dict = get_percent_error_dict(full_workload_stat_dict, cur_workload_stat_dict)
        percent_error_dict["region"] = region_index
        new_workload_stat_arr.append(percent_error_dict)
        if num_blk_evaluated % 5 == 0:
            time_len = perf_counter() - start_time 
            print("{}, {}% completed in {}.".format(num_blk_evaluated, int(100*num_blk_evaluated/len(blk_addr_list)), time_len))

    return DataFrame(new_workload_stat_arr)


def remove_block(
        workload_stat_dict: dict,
        per_blk_access_stat_dict: dict, 
        blk_addr: int,
        first_blk_req: BlkReq,
        second_blk_req: BlkReq, 
        blk_size_byte: int = 512, 
        update_stat_dict = True
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
    # first_blk_req_dict = get_first_block_req(per_blk_access_stat_dict)
    if first_blk_req.is_solo_req(blk_addr):
        # second_blk_req_dict = get_first_block_req(per_blk_access_stat_dict, filter_index=first_blk_req_dict["min_index"])
        if second_blk_req.op == 'r':
            new_workload_stat_dict["total_read_iat"] -= (second_blk_req.iat - first_blk_req.iat)
        elif second_blk_req.op == 'w':
            new_workload_stat_dict["total_write_iat"] -= (second_blk_req.iat - first_blk_req.iat)
        else:
            raise ValueError("Unrecognized request type {}.".format(second_blk_req.op))

    new_workload_stat_dict["write_ratio"] = new_workload_stat_dict["write_count"]/(new_workload_stat_dict["read_count"] + new_workload_stat_dict["write_count"])
    new_workload_stat_dict["mean_read_size"] = new_workload_stat_dict["total_read_size"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_size"] = new_workload_stat_dict["total_write_size"]/new_workload_stat_dict["write_count"]
    new_workload_stat_dict["mean_read_iat"] = new_workload_stat_dict["total_read_iat"]/new_workload_stat_dict["read_count"]
    new_workload_stat_dict["mean_write_iat"] = new_workload_stat_dict["total_write_iat"]/new_workload_stat_dict["write_count"]

    left_accessed, right_accessed = blk_addr-1 in per_blk_access_stat_dict, blk_addr+1 in per_blk_access_stat_dict
    if left_accessed and update_stat_dict:
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
    
    if right_accessed and update_stat_dict:
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
    
    if update_stat_dict:
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