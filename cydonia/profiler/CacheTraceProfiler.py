from pathlib import Path 
from pandas import read_csv, DataFrame 

from cydonia.profiler.CPReader import CPReader


DEFAULT_COMPARE_FEATURE_LIST = ["write_block_req_split", "write_cache_req_split", "iat_read_avg", "iat_write_avg",
                                    "read_size_avg", "write_size_avg", "read_misalignment_per_req", "write_misalignment_per_req"]


def validate_cache_trace(
        block_trace_path: Path,
        cache_trace_path: Path
) -> bool:
    block_feature_dict = get_workload_feature_dict_from_block_trace(block_trace_path)
    cache_trace_df = load_cache_trace(cache_trace_path)
    cache_feature_dict = get_workload_feature_dict_from_cache_trace(cache_trace_df)
    for feature_name in block_feature_dict:
        assert block_feature_dict[feature_name]==cache_feature_dict[feature_name],\
            "Feature {} not equal in block trace {} and cache trace {}.".format(feature_name, 
                                                                                    block_feature_dict[feature_name], 
                                                                                    cache_feature_dict[feature_name])


def load_cache_trace(cache_trace_path: Path) -> DataFrame:
    """Load a cache trace file into a DataFrame.

    Args:
        cache_trace_path: Path to cache trace.

    Returns:
        cache_trace_df: DataFrame with cache trace.
    """
    return read_csv(cache_trace_path, 
                        names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])

        
def generate_block_trace(
        cache_trace_path: Path, 
        block_trace_path: Path,
        lba_size_byte: int = 512, 
        block_size_byte: int = 4096
) -> None:
    """Generate block trace from a cache trace.

    Args:
        cache_trace_path: Path to the cache trace.
        block_trace_path: Path to the block trace.
        lba_size_byte: Size of each LBA in byte.
        block_size_byte: Size of a cache block in byte. 
    """
    cur_ts = 0
    cache_trace_df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
    with block_trace_path.open("w+") as block_trace_handle:
        for _, group_df in cache_trace_df.groupby(by=['i']):
            # Get the list of block requests generated from this group of cache requests that belong to the
            # same souce block request in the full trace. 
            sorted_group_df = group_df.sort_values(by=["key"])
            write_group_df = sorted_group_df[sorted_group_df["op"] == 'w']
            if len(write_group_df) == 0:
                read_group_df = sorted_group_df[sorted_group_df["op"] == 'r']
                block_req_arr = get_block_reqs_from_cache_reqs(read_group_df, block_size_byte, lba_size_byte)
            else:
                block_req_arr = get_block_reqs_from_cache_reqs(write_group_df, block_size_byte, lba_size_byte)
            
            # Write block requests generated from the set of cache accesses to the new block trace. 
            for cur_block_req in block_req_arr:
                cur_ts += int(cur_block_req["iat"])
                assert int(cur_block_req["size"]) >= lba_size_byte, "Size too small {}.".format(int(cur_block_req["size"]))
                block_trace_handle.write("{},{},{},{}\n".format(cur_ts, 
                                                                    int(cur_block_req["lba"]), 
                                                                    cur_block_req["op"], 
                                                                    int(cur_block_req["size"])))


def get_block_reqs_from_cache_reqs(
        cache_req_df: DataFrame,
        block_size_byte: int,
        lba_size_byte: int
) -> list:
    """Get block requests from a set of cache requests.

    Args:
        cache_req_df: DataFrame of cache requests from which to generate block requests.
        block_size_byte: Size of a cache block.
        lba_size_byte: Size of an LBA. 
    
    Returns:
        block_req_arr: List of block requests represented by dicts.
    """
    block_req_arr = []
    prev_req = cache_req_df.iloc[0]
    cur_req_start_byte = (int(prev_req["key"]) * block_size_byte) + int(prev_req["front_misalign"])
    cur_req_size = block_size_byte - prev_req["front_misalign"]
    for _, cur_req in cache_req_df.iloc[1:].iterrows():
        if cur_req["key"] == (prev_req["key"]+1):
            cur_req_size += block_size_byte
        else:
            assert cur_req_start_byte % lba_size_byte == 0, \
                "Req start byte is {} and lba size byte is {}".format(cur_req_start_byte, lba_size_byte)
            assert cur_req_size % lba_size_byte == 0 and cur_req_size > 0
            block_req_arr.append({
                "iat": cur_req["iat"],
                "lba": int(cur_req_start_byte/lba_size_byte),
                "size": cur_req_size,
                "op": cur_req["op"]
            })
            cur_req_start_byte = int(cur_req["key"]) * block_size_byte
            cur_req_size = block_size_byte
        
        prev_req = cur_req 
    
    cur_req_size -= prev_req["rear_misalign"]
    assert cur_req_start_byte % lba_size_byte == 0
    assert cur_req_size % lba_size_byte == 0 and cur_req_size > 0
    block_req_arr.append({
        "iat": prev_req["iat"],
        "lba": int(cur_req_start_byte/lba_size_byte),
        "size": cur_req_size,
        "op": prev_req["op"]
    })

    return block_req_arr


def get_workload_feature_dict_from_cache_trace(
        trace_df: DataFrame,
        block_size_byte: int = 4096,
        lba_size_byte: int = 512 
) -> dict:
    """Get a dictionary of workload features from a cache trace.

    Args:
        cache_trace_path: Path to cache trace.
        block_size_byte: Size of a cache block in bytes. (Default: 4096)
    """

    read_iat_us, write_iat_us = 0, 0 
    read_block_req_byte, write_block_req_byte = 0, 0 
    read_misalign_byte, write_misalign_byte = 0, 0 
    read_cache_req_count, write_cache_req_count = 0, 0
    read_block_req_count, write_block_req_count = 0, 0 

    for _, group_df in trace_df.groupby(by=["i"]):
        read_group_df = group_df[group_df["op"] == 'r']
        write_group_df = group_df[group_df["op"] == 'w']

        read_cache_req_count += len(read_group_df)
        write_cache_req_count += len(write_group_df)

        if len(write_group_df) == 0:
            sum_front_misalign = read_group_df["front_misalign"].sum()
            sum_rear_misalign = read_group_df["rear_misalign"].sum()
            block_req_arr = get_block_reqs_from_cache_reqs(read_group_df, block_size_byte, lba_size_byte)
            
            if read_block_req_count + write_block_req_count > 0:
                read_iat_us += (group_df.iloc[0]["iat"] * len(block_req_arr))
            else:
                read_iat_us += (group_df.iloc[0]["iat"] * (len(block_req_arr)-1))
            
            read_block_req_count += len(block_req_arr) 
            read_block_req_byte += (sum([req["size"] for req in block_req_arr]))
            read_misalign_byte += (sum_front_misalign + sum_rear_misalign)
        else:
            sum_front_misalign = write_group_df["front_misalign"].sum()
            sum_rear_misalign = write_group_df["rear_misalign"].sum()
            block_req_arr = get_block_reqs_from_cache_reqs(write_group_df, block_size_byte, lba_size_byte)
            
            if read_block_req_count + write_block_req_count > 0:
                write_iat_us += (group_df.iloc[0]["iat"] * len(block_req_arr))
            else:
                write_iat_us += (group_df.iloc[0]["iat"] * (len(block_req_arr)-1))

            write_block_req_count += len(block_req_arr) 
            write_block_req_byte += (sum([req["size"] for req in block_req_arr]))
            write_misalign_byte += (sum_front_misalign + sum_rear_misalign)
    
    read_cache_req_byte = read_cache_req_count * block_size_byte
    write_cache_req_byte = write_cache_req_count * block_size_byte
    write_cache_req_ratio = write_cache_req_byte/(read_cache_req_byte + write_cache_req_byte)
    write_req_ratio = write_block_req_count/(read_block_req_count + write_block_req_count)
    return {
        "write_block_req_split": write_req_ratio,
        "write_cache_req_split": write_cache_req_ratio,
        "iat_read_avg": read_iat_us/read_block_req_count if read_block_req_count > 0 else 0,
        "iat_write_avg": write_iat_us/write_block_req_count if write_block_req_count > 0 else 0,
        "read_size_avg": read_block_req_byte/read_block_req_count if read_block_req_count > 0 else 0,
        "write_size_avg": write_block_req_byte/write_block_req_count if write_block_req_count > 0 else 0,
        "read_misalignment_per_req": read_misalign_byte/read_block_req_count if read_block_req_count > 0 else 0,
        "write_misalignment_per_req": write_misalign_byte/write_block_req_count if write_block_req_count > 0 else 0,
        "read_block_req_count": read_block_req_count,
        "write_block_req_count": write_block_req_count,
        "read_cache_req_count": read_cache_req_count,
        "write_cache_req_count": write_cache_req_count,
        "iat_read_sum": read_iat_us,
        "iat_write_sum": write_iat_us,
        "read_misalignment_sum": read_misalign_byte,
        "write_misalignment_sum": write_misalign_byte,
        "read_block_req_byte": read_block_req_byte,
        "write_block_req_byte": write_block_req_byte
    }


def get_workload_feature_dict_from_block_trace(
        block_trace_path: Path, 
        block_size_byte: int = 4096
) -> dict:
    feature_dict =  {
        "read_block_req_count": 0,
        "write_block_req_count": 0,
        "read_cache_req_count": 0,
        "write_cache_req_count": 0,
        "iat_read_sum": 0.0,
        "iat_write_sum": 0,
        "read_misalignment_sum": 0,
        "write_misalignment_sum": 0,
        "read_block_req_byte": 0,
        "write_block_req_byte": 0
    }
    reader = CPReader(block_trace_path)
    cur_block_req = reader.get_next_block_req(block_size=block_size_byte)
    prev_ts = cur_block_req["ts"]
    while cur_block_req:
        start_block = cur_block_req["start_block"]
        end_block = cur_block_req["end_block"]
        cur_ts = cur_block_req["ts"]
        size_byte = cur_block_req["size"]
        iat = cur_ts - prev_ts
        assert start_block <= end_block
        if cur_block_req["op"] == 'r':
            feature_dict["read_block_req_count"] += 1 
            feature_dict["read_block_req_byte"] += size_byte
            feature_dict["read_cache_req_count"] += (cur_block_req["end_block"] + 1 - cur_block_req["start_block"])
            feature_dict["iat_read_sum"] += iat 
            feature_dict["read_misalignment_sum"] += (cur_block_req["front_misalign"] + cur_block_req["rear_misalign"])
        else:
            feature_dict["write_block_req_count"] += 1 
            feature_dict["write_block_req_byte"] += size_byte
            feature_dict["write_cache_req_count"] += (cur_block_req["end_block"] + 1 - cur_block_req["start_block"])
            feature_dict["iat_write_sum"] += iat 
            feature_dict["write_misalignment_sum"] += (cur_block_req["front_misalign"] + cur_block_req["rear_misalign"])

            if cur_block_req["start_block"] == cur_block_req["end_block"]:
                if cur_block_req["front_misalign"] > 0 or cur_block_req["rear_misalign"] > 0:
                    feature_dict["read_cache_req_count"] += 1 
            else:
                if cur_block_req["front_misalign"] > 0:
                    feature_dict["read_cache_req_count"] += 1
                if cur_block_req["rear_misalign"] > 0:
                    feature_dict["read_cache_req_count"] += 1
        prev_ts = cur_ts
        cur_block_req = reader.get_next_block_req(block_size=block_size_byte)
    reader.trace_file_handle.close()
    feature_dict["write_block_req_split"] = feature_dict["write_block_req_count"]/(feature_dict["write_block_req_count"]+feature_dict["read_block_req_count"])
    feature_dict["write_cache_req_split"] = feature_dict["write_cache_req_count"]/(feature_dict["write_cache_req_count"]+feature_dict["read_cache_req_count"])
    feature_dict["iat_read_avg"] = feature_dict["iat_read_sum"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["iat_write_avg"] = feature_dict["iat_write_sum"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    feature_dict["read_size_avg"] = feature_dict["read_block_req_byte"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["write_size_avg"] = feature_dict["write_block_req_byte"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    feature_dict["read_misalignment_per_req"] = feature_dict["read_misalignment_sum"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["write_misalignment_per_req"] = feature_dict["write_misalignment_sum"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    return feature_dict 