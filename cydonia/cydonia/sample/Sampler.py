""" This class generates samples from a block storage traces based on user specified params. 
"""

import pathlib 
import time
import copy 
import mmh3
import pandas as pd 
import numpy as np 
from collections import Counter


class Sampler:
    def __init__(self, trace_path, lba_size=512):
        """ Init the Sampler class with path of block trace and the size of LBAs to be sampled 

            Parameters
            ----------
            trace_path : pathlib.Path/str 
                path to the block trace to be sampled 
            lba_size : int 
                size of a LBA (Logical Block Address) in bytes (Default: 512)
        """
        # max hash val for 128 bit mmh3
        self.max_hash_val = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

        # size of a logical block address in the trace (usually 512 bytes)
        self.lba_size = lba_size

        self.trace_path = pathlib.Path(trace_path)
        assert(self.trace_path.exists(), 
                "Trace path {} provided does not exist!".format(self.trace_path))

        self.block_trace_df = pd.read_csv(self.trace_path, names=['ts', 'lba', 'op', 'size'])
        self.block_trace_df['iat'] = self.block_trace_df["ts"].diff()

        # the 'iat' of the first request will be NA so replace it with 0 
        self.block_trace_df.fillna(0, inplace=True)

    
    def ignore_n_low_order_bits(self, val, n):
        return val >> n


    def sample_row_dict_to_str(self, sample_dict):
        return "{},{},{},{}\n".format(sample_dict['ts'], sample_dict['lba'], sample_dict['op'], sample_dict['size'])


    def sample(self, rate, seed, bits, ts_method, sample_path):
        """ Sample using the specified rate, random seed, ts generation 
            method while ignoring specific bits of the address when 
            hashing such that multiple addresses could map to the 
            same hash. 

            Parameters
            ----------
            rate : float 
                sampling rate 
            seed : int 
                random seed 
            bits : list(int)
                list of bits to ignore when hashing 
            ts_method : str 
                the method to generate timestamps in the sample 
            sample_path : pathlib.Path
                path to the sample file 

            Return 
            ------
            sample_df : pd.DataFrame 
                pandas DataFrame of the sample block trace 
            split_counter : collections.Counter
                count of the number of samples generated per block request sampled 
        """
        start_time = time.time()
        sample_file_handle = sample_path.open("w+")

        # define the limit based on the rate 
        # if the hash value is under this limit, we sample 
        limit = self.max_hash_val * rate 
        
        """ A single multi-block request in the original block 
            trace can generate multiple possibly multi-block 
            requests in the sample. This happens when a fragment 
            of the blocks in a multi-block request are sampled. 

            This Counter tracks the number of samples generated
            per block request that had at least 1 block sampled. """
        split_counter = Counter()

        prev_sample_req_ts = 0 
        """ Track how sample count changes after evaluating each 
            block request in order to track if a single block request 
            led to multiple sample block requests. The change in sample 
            count after each evaluation of block request is also a trigger
            to update the timestamp of the next sample block request. """
        sample_count = 0 
        prev_sample_count = 0 
        for row_index, row in self.block_trace_df.iterrows():
            # the blocks that the block request touches 
            lba_start = row['lba']
            lba_end = lba_start + int(row['size']/self.lba_size)

            cur_sample_block_req = {
                'ts': 0,
                'lba': 0,
                'op': 'r',
                'size': 0
            }
            # print update every 100,000 block request processed 
            if row_index % 1e5 == 0:
                end_time = time.time()
                process_time = end_time - start_time 
                print("prog->{} processed in {:.5f} minutes".format(row_index, float(process_time/60.0)))

            for cur_lba in range(lba_start, lba_end):
                # map the LBA to a new value by ignoring specified bits 
                addr = self.ignore_n_low_order_bits(cur_lba, bits)

                # mask the address where bits are ignored 
                hash_val = mmh3.hash128(str(addr), signed=False, seed=seed)

                # the timestamp to be used for samples generated if this block request is sampled 
                sample_ts = int(prev_sample_req_ts + row['iat'])
                if hash_val < limit:
                    # sample this block 
                    if cur_sample_block_req['size'] == 0:
                        # we were not tracking any sample block request 
                        # so start tracking  
                        if ts_method == 'iat' or ts_method == 'iat0':
                            cur_sample_block_req['ts'] = int(prev_sample_req_ts + row['iat'])
                        elif ts_method == 'iatscale':
                            cur_sample_block_req['ts'] = int(prev_sample_req_ts + (row['iat']*rate))
                        else:
                            raise ValueError("Unknown ts method {}".format(ts_method))
                        
                        cur_sample_block_req['lba'] = int(cur_lba)
                        cur_sample_block_req['op'] = row['op']
                        cur_sample_block_req['size'] = self.lba_size
                    else:
                        # we were already tracking so update the size 
                        # this means contiguous blocks were sampled 
                        cur_sample_block_req['size'] += self.lba_size
                else:
                    # dont sample this block 
                    if cur_sample_block_req['size'] > 0:
                        # we were tracking a sample block request 
                        # this means that the streak of contiguous 
                        # blocks being sampled was broken so we 
                        # need to create a sample block request 
                        sample_file_handle.write(self.sample_row_dict_to_str(cur_sample_block_req))
                        sample_count += 1

                        if ts_method == "iat":
                            prev_sample_req_ts = int(prev_sample_req_ts + row['iat'])
                        elif ts_method == "iatscale":
                            prev_sample_req_ts = int(prev_sample_req_ts + (row['iat']*rate))

                        # reset the sample block request 
                        cur_sample_block_req = {
                            'ts': 0,
                            'lba': 0,
                            'op': 'r',
                            'size': 0
                        }

            # we might exit while tracking a sample block request we never recorded 
            if cur_sample_block_req['size'] > 0:
                sample_file_handle.write(self.sample_row_dict_to_str(cur_sample_block_req))
                sample_count += 1

                if ts_method == "iat":
                    prev_sample_req_ts = int(prev_sample_req_ts + row['iat'])
                elif ts_method == "iatscale":
                    prev_sample_req_ts = int(prev_sample_req_ts + (row['iat']*rate))
            
            # check if we generated any sample from this block request 
            if sample_count > prev_sample_count:
                split_counter[sample_count- prev_sample_count] += 1
                if ts_method == "iat0":
                    prev_sample_req_ts = int(prev_sample_req_ts + row['iat'])
            
            # update sample count 
            prev_sample_count = sample_count
        
        sample_file_handle.close()
        end_time = time.time()
        process_time = end_time - start_time 
        print("Done! prog->{} sampled in {:.5f} minutes".format(self.trace_path, float(process_time/60.0)))
        return split_counter