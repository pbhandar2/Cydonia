from pathlib import Path 
from json import JSONEncoder, dumps
from numpy import ndarray, int64 
from pandas import read_csv, DataFrame 

from cydonia.profiler.CPReader import CPReader


DEFAULT_COMPARE_FEATURE_LIST = ["write_block_req_split", "write_cache_req_split", "iat_read_avg", "iat_write_avg",
                                    "read_size_avg", "write_size_avg", "read_misalignment_per_req", "write_misalignment_per_req"]


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, int64):
            return int(obj)
        return JSONEncoder.default(self, obj)


def validate_cache_trace(
        block_trace_path: Path,
        cache_trace_path: Path
) -> None:
    """Validate cache trace by generating features from the cache trace and block trace and comparing them.

    Args:
        block_trace_path: Path of block trace.
        cache_trace_path: Path of cache trace.
    """
    block_feature_dict = get_workload_feature_dict_from_block_trace(block_trace_path)
    cache_trace_df = load_cache_trace(cache_trace_path)
    cache_feature_dict = get_workload_feature_dict_from_cache_trace(cache_trace_df)
    for feature_name in block_feature_dict:
        if block_feature_dict[feature_name]!=cache_feature_dict[feature_name]:
            print("Found unequal feature from block trace {} and cache trace {}.".format(block_trace_path, cache_trace_path))
            print(dumps(block_feature_dict, indent=2, cls=NumpyEncoder))
            print(dumps(cache_feature_dict, indent=2, cls=NumpyEncoder))
            raise ValueError("Feature {} not equal in block trace {} and cache trace {}.".format(feature_name, 
                                                                                    block_feature_dict[feature_name], 
                                                                                    cache_feature_dict[feature_name]))
        
    print("Validated block trace {} and cache trace {}.".format(block_trace_path, cache_trace_path))
    print(dumps(block_feature_dict, indent=2, cls=NumpyEncoder))
    print(dumps(cache_feature_dict, indent=2, cls=NumpyEncoder))


def load_cache_trace(cache_trace_path: Path) -> DataFrame:
    """Load a cache trace file into a DataFrame.

    Args:
        cache_trace_path: Path to cache trace.

    Returns:
        cache_trace_df: DataFrame with cache trace.
    """
    return read_csv(cache_trace_path, 
                        names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])


def get_block_req_arr(
        cache_req_df: DataFrame, 
        lba_size_byte: int, 
        block_size_byte: int
) -> list:
    """Get block requests from a set of cache requests originating from the
    same source block request.

    Args:
        cache_req_df: DataFrame containing a set of cache requests.
        lba_size_byte: Size of an LBA in bytes. 
        block_size_byte: Size of a cache block in bytes.
    
    Returns:
        block_req_arr: List of dictionary with attributes of each block request.
    """
    block_req_arr = []
    if not cache_req_df["op"].str.contains('w').any():
        cur_cache_req_df = cache_req_df
        cur_op = 'r'
    else:
        cur_cache_req_df = cache_req_df[cache_req_df["op"] == 'w']
        cur_op = 'w'

    # handle the misalignment possible in the first block accessed
    first_cache_req = cur_cache_req_df.iloc[0]
    iat_us = first_cache_req["iat"]
    prev_key = first_cache_req["key"]
    rear_misalign_byte = first_cache_req["rear_misalign"]
    req_start_byte = (first_cache_req["key"] * block_size_byte) + first_cache_req["front_misalign"]

    req_size_byte = block_size_byte - first_cache_req["front_misalign"]
    for _, row in cur_cache_req_df.iloc[1:].iterrows():
        cur_key = row["key"]
        if cur_key - 1 == prev_key:
            req_size_byte += block_size_byte
        else:
            block_req_arr.append({
                "iat": iat_us,
                "lba": int(req_start_byte/lba_size_byte),
                "size": req_size_byte,
                "op": cur_op
            })
            req_start_byte = cur_key * block_size_byte
            req_size_byte = block_size_byte
        rear_misalign_byte = row["rear_misalign"]
        prev_key = cur_key
    
    block_req_arr.append({
        "iat": iat_us,
        "lba": int(req_start_byte/lba_size_byte),
        "size": int(req_size_byte - rear_misalign_byte),
        "op": cur_op
    })
    assert all([req["size"]>0 for req in block_req_arr]), "All sizes not greater than 0, found {}.".format(block_req_arr)
    return block_req_arr
        

def generate_block_trace(
        cache_trace_path: Path, 
        block_trace_path: Path,
        lba_size_byte: int = 512, 
        block_size_byte: int = 4096
) -> None:
    """Generate block trace from cache trace.

    Args:
        cache_trace_path: Path of the cache trace.
        block_trace_path: Path of the block trace to be generated.
        lba_size_byte: Size of an LBA in bytes. 
        block_size_byte: Size of a cache block in bytes. 
    """
    cur_ts = 0
    cache_trace_df = load_cache_trace(cache_trace_path)
    with block_trace_path.open("w+") as block_trace_handle:
        for _, group_df in cache_trace_df.groupby(by=['i']):
            sorted_group_df = group_df.sort_values(by=["key"])
            block_req_arr = get_block_req_arr(sorted_group_df, lba_size_byte, block_size_byte)

            for cur_block_req in block_req_arr:
                cur_ts += int(cur_block_req["iat"])
                assert int(cur_block_req["size"]) >= lba_size_byte, "Size too small {}.".format(int(cur_block_req["size"]))
                block_trace_handle.write("{},{},{},{}\n".format(cur_ts, int(cur_block_req["lba"]), cur_block_req["op"], int(cur_block_req["size"])))


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
            block_req_arr = get_block_req_arr(read_group_df, lba_size_byte, block_size_byte)
            
            if read_block_req_count + write_block_req_count > 0:
                read_iat_us += (group_df.iloc[0]["iat"] * len(block_req_arr))
            else:
                # First block request will have IAT of 0
                read_iat_us += (group_df.iloc[0]["iat"] * (len(block_req_arr)-1))
            
            read_block_req_count += len(block_req_arr) 
            read_block_req_byte += (sum([req["size"] for req in block_req_arr]))
            read_misalign_byte += (sum_front_misalign + sum_rear_misalign)
        else:
            sum_front_misalign = write_group_df["front_misalign"].sum()
            sum_rear_misalign = write_group_df["rear_misalign"].sum()
            block_req_arr = get_block_req_arr(write_group_df, lba_size_byte, block_size_byte)
            
            if read_block_req_count + write_block_req_count > 0:
                write_iat_us += (group_df.iloc[0]["iat"] * len(block_req_arr))
            else:
                # First block request will have IAT of 0
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
    reader.close()
    feature_dict["write_block_req_split"] = feature_dict["write_block_req_count"]/(feature_dict["write_block_req_count"]+feature_dict["read_block_req_count"])
    feature_dict["write_cache_req_split"] = feature_dict["write_cache_req_count"]/(feature_dict["write_cache_req_count"]+feature_dict["read_cache_req_count"])
    feature_dict["iat_read_avg"] = feature_dict["iat_read_sum"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["iat_write_avg"] = feature_dict["iat_write_sum"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    feature_dict["read_size_avg"] = feature_dict["read_block_req_byte"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["write_size_avg"] = feature_dict["write_block_req_byte"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    feature_dict["read_misalignment_per_req"] = feature_dict["read_misalignment_sum"]/feature_dict["read_block_req_count"] if feature_dict["read_block_req_count"] > 0 else 0 
    feature_dict["write_misalignment_per_req"] = feature_dict["write_misalignment_sum"]/feature_dict["write_block_req_count"] if feature_dict["write_block_req_count"] > 0 else 0 
    return feature_dict 