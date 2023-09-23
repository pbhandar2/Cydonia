"""This script generates block traces for testing."""

from pandas import DataFrame, concat 
from numpy import random


def get_blk_trace_with_solo_req_to_start(
        approx_num_blk_req: int,
        num_unique_blk_req: int,
        num_solo_req_to_start: int = 1,
        op: str = 'r',
        blk_size_byte: int = 512,
        max_iat_us: int = 1000000
) -> DataFrame:
    blk_req_arr_to_add = []
    ts_us_tracker = 0 
    block_addr = random.randint(low=1, high=num_unique_blk_req)
    for _ in range(num_solo_req_to_start):
        blk_req_arr_to_add.append({
            "ts": ts_us_tracker,
            "lba": block_addr,
            "op": op,
            "size": blk_size_byte
        })
        ts_us_tracker += random.randint(low=1, high=max_iat_us)
    
    # add the max ts plus a random iat to all ts in trace before inserting the new request in the front 
    max_new_ts = blk_req_arr_to_add[-1]["ts"]
    blk_trace_df = generate_random_blk_trace(approx_num_blk_req, num_unique_blk_req)
    blk_trace_df["ts"] += (max_new_ts + random.randint(low=1, high=max_iat_us))
    new_blk_trace = concat([DataFrame(blk_req_arr_to_add), blk_trace_df], ignore_index=True).sort_values(by=["ts"])
    return new_blk_trace


def generate_random_blk_trace(
        approx_num_blk_req: int,
        num_unique_blk_req: int,
        max_iat_us: int = 1000000,
        max_size_block: int = 8,
        blk_size_byte: int = 512 
) -> DataFrame:
    """Generate a DataFrame of random block trace. 

    Args:
        approx_num_blk_req: Approximate number of block requests in the sample. In order to guarentee all unique blocks will be included,
                                we currently cannot guarentee that the number of block request in the test sample will be exactly as specified. 
        num_unique_blk_req: Number of unique blocks in the test block trace. 
    
    Returns:
        random_blk_trace_df: DataFrame of a test block trace. 
    """
    ts_us_tracker = 0
    blk_req_dict_arr = []
    unique_lba_set = set()
    for _ in range(approx_num_blk_req):
        blk_req_dict = {
            "ts": ts_us_tracker,
            "lba": random.randint(low=1, high=num_unique_blk_req),
            "op": 'r' if random.randint(low=0, high=1) == 0 else 'w',
            "size": random.randint(low=1, high=max_size_block) * blk_size_byte
        }
        for blk_addr in range(blk_req_dict["lba"], blk_req_dict["lba"]+int(blk_req_dict["size"]/blk_size_byte)):
            unique_lba_set.add(blk_addr)

        blk_req_dict_arr.append(blk_req_dict)
        ts_us_tracker += random.randint(low=1, high=max_iat_us)
    
    # if few blocks have not yet been included, then add them manually 
    full_block_addr_set = set(range(num_unique_blk_req))
    remaining_block_addr_set = full_block_addr_set.difference(unique_lba_set)
    for remaining_block_addr in remaining_block_addr_set:
        blk_req_dict = {
            "ts": ts_us_tracker,
            "lba": remaining_block_addr,
            "op": 'r' if random.randint(low=0, high=1) == 0 else 'w',
            "size": random.randint(low=1, high=max_size_block) * blk_size_byte
        }
        blk_req_dict_arr.append(blk_req_dict)
        ts_us_tracker += random.randint(low=1, high=max_iat_us)
    
    blk_trace_df = DataFrame(blk_req_dict_arr)
    assert blk_trace_df["ts"].is_monotonic_increasing, \
        "Timestamp in test block trace not monotonically increasing."

    return blk_trace_df