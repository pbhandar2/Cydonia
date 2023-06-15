""" This class provides different sampling functions for block storage traces. 
"""

import mmh3
import numpy as np 
import pandas as pd 


# max 128 bit signed for hashing 
MAX_HASH_VAL = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 
SECTOR_SIZE = 512


def get_unique_addr_array(df, ignore_bits):
    """ Get the unique addresses accessed in a block trace loaded in DataFrame. 

        Parameters
        ----------
        df (pd.DataFrame) : the DataFrame containing the block trace 

        Return 
        ------
        unique_addr_list (np.ndarray) : list of unique addresses accessed in the block trace 
    """
    unique_addr_set = set()
    for index, row in df.iterrows():
        for lba in range(row['lba'], row['lba'] + int(row['size']/SECTOR_SIZE)):
            if ignore_bits is None:
                unique_addr_set.add(lba)
            else:
                unique_addr_set.add(int(min(mask_equiv_bits(lba, ignore_bits))))
    return np.array(list(unique_addr_set), dtype=int)


def load_block_storage_trace(trace_path):
    df = pd.read_csv(trace_path, names=["ts", "lba", "op", "size"]) 
    df['iat'] = df["ts"].diff()
    return df


def sample_block_trace(df, sampled_addr_array, ignore_bits, ts_method):
    """ Sample the block trace in the DataFrame by rows with the provided addresses and generate timestamps
        by the specified method. 

        Parameters
        ----------
        df (pd.DataFrame) : the DataFrame of block trace 
        sampled_addr_array (np.ndarray) : array of addresses that should be sampled 
        ignore_bits (list) : list of bits to ignore
        ts_method (str) : type of method used to generate timestamps

        Return 
        ------
        sample_df (pd.DataFrame) : the DataFrame of sample trace 
        split_percent (int) : percentage of sampled block request that were split to multiple requests in the sample 
    """
    rows_sampled = 0 
    sample_split_count = 0 
    curTs = 0
    sample_row_list = []

    for index, row in df.iterrows():
        # get the timestamp 
        ts = row['ts']
        if ts_method == "iat":
            if not np.isnan(row['iat']):
                curTs += row['iat']
            ts = curTs 
        
        start_lba = None
        size = 0 
        samples_generated = 0
        for lba in range(row['lba'], row['lba'] + int(row['size']/SECTOR_SIZE)):

            if ignore_bits is None:
                addr = lba
            else:
                addr = int(min(mask_equiv_bits(lba, ignore_bits)))
            
            if addr in sampled_addr_array:
                # sample this lba 
                if start_lba is not None:
                    # we have been tracking a block req
                    size += SECTOR_SIZE
                else:
                    # we have not been tracking a block req, lets start tracking 
                    start_lba = lba 
                    size = SECTOR_SIZE 
            else:
                # dont sample this lba 
                if start_lba is not None:
                    # we have been tracking a block req, but continuity is broken so create a sample block req 
                    sample_row_list.append({
                        'ts': ts,
                        'lba': start_lba,
                        'size': size,
                        'op': row['op']
                    })
                    samples_generated += 1

                    # reset tracking 
                    size = 0 
                    start_lba = None 
        
            if start_lba is not None:
                sample_row_list.append({
                    'ts': ts,
                    'lba': start_lba,
                    'size': size,
                    'op': row['op']
                })
                samples_generated += 1
        
        if samples_generated >= 1:
            rows_sampled += 1
            if samples_generated > 1:
                sample_split_count += 1

    sample_df = pd.DataFrame(sample_row_list)
    return sample_df, int(100*sample_split_count/rows_sampled)


def generate_samples(trace_path, rate_array, seed, ignore_bits, ts_method, out_dir):
    """ Generate sample block traces in the given directories based on the given parameters. 

        Parameters
        ----------
        trace_path (pathlib.Path) : path to original trace file 
        rate_array (np.ndarray) : array of sampling rates in range (0.0,100.0) 
        seed (int) : random seed
        ignore_bits (list) : list of bits to ignore
        ts_method (str) : type of method used to generate timestamps
        out_dir (pathlib.Path) : directory where samples will be output
    """
    df = load_block_storage_trace(trace_path)
    unique_addr_array = get_unique_addr_array(df, ignore_bits)

    if ignore_bits is None:
        ignore_bits_str = 'NA'
    else:
        ignore_bits_str = "-".join(ignore_bits)
    
    # generate samples and store the number of splits 
    for rate in rate_array:
        sampled_addr_array = random_spatial_sampling(unique_addr_array, rate, seed)
        sample_df, split_percent = sample_block_trace(df, sampled_addr_array, ignore_bits, ts_method)

        out_dir.joinpath(ts_method).mkdir(exist_ok=True)
        workload_name = trace_path.stem 
        output_file_name = "{}_{}_{}_{}_{}.csv".format(workload_name, int(rate*100), seed, split_percent, ignore_bits_str)
        out_path = out_dir.joinpath(ts_method, output_file_name)
        print(out_path)
        sample_df.to_csv(out_path, index=False)


def random_spatial_sampling(lba_array, rate, seed):
    """ Randomized spatial sampling of logical block addresses (LBAs) in a trace. 

        Parameters
        ----------
        lba_array (np.ndarray): array of LBAs 
        rate (float) : sampling rate in range (0.0,100.0) 
        seed (int) : random seed

        Return 
        ------
        sampled_lba_array (np.ndarray): array of sampled LBAs 
    """
    # array of flags for each element in "lba_array" indicating whether or not it was sampled 
    sample_flag_array = np.zeros(len(lba_array), dtype=int)

    limit = MAX_HASH_VAL * rate 
    for index, lba in enumerate(lba_array):
        hash_val = mmh3.hash128(str(lba), signed=False, seed=seed)
        if hash_val < limit:
            # this LBA is sampled! 
            sample_flag_array[index] = 1
    
    # all LBAs in "lba_array" in indexes where "sample_flag_array" is nonzero are sampled 
    return np.array(lba_array[np.flatnonzero(sample_flag_array)], dtype=int)


""" Function mask_equiv_bit and mask_equiv_bits by Carl Waldspurger. 
    Code taken from a screenshot in email sent by
    Carl Waldspurger on (Sat, 19 Jun 2021 12:49:49 -0700) with 
    subject "non-contiguous locality sampling". 
"""
def mask_equiv_bit(addr, bit):
    """ Return address equivalent to addr while ignoring specified bit.
        
        Parameters
        ----------
        addr (int) : the address (could be Logical Block Address)
        bit (int) : the bit to ignore 

        Return 
        ------
        equiv_addr (int) : the address equivalent to "addr" while ignoring specified bit 
    """
    mask = 1 << bit;
    if addr & mask:
        return addr & ~mask
    else:
        return addr | mask 


def mask_equiv_bits(addr, bits):
    """ Return addresses equivalent to addr while ignoring specified bits.
        
        Parameters
        ----------
        addr (int) : the address (could be Logical Block Address)
        bits (list) : list of bits to ignore

        Return 
        ------
        equiv_addr_list (list) : sorted list of addresses equivalent to "addr" while ignoring specified bits 
    """
    equiv_list = [addr]
    for b in bits:
        equiv_list += [mask_equiv_bit(e, b) for e in equiv_list]
    return sorted(equiv_list)