""" This class generates samples from a block storage traces based on user specified params. 
"""

import pathlib 
import time
import copy 
import mmh3
import pandas as pd 
import numpy as np 
from collections import Counter

from cydonia.sample.util import mask_equiv_bits


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


    def sample(self, rate, seed, bits, ts_method):
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

            Return 
            ------
            sample_df : pd.DataFrame 
                pandas DataFrame of the sample block trace 
            split_counter : collections.Counter
                count of the number of samples generated per block request sampled 
        """
        start_time = time.time()

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

        ts_tracker = 0 # track the timestamp of block request 
        prev_sample_count = 0 # track the number of block request sampled 
        row_json_list = [] # list of rows in sample block request as JSON 
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

            # print update every millions block request processed 
            if row_index % 1e6 == 0:
                end_time = time.time()
                process_time = end_time - start_time 
                print("prog->{} processed in {:.5f} minutes".format(row_index, float(process_time/60.0)))

            for cur_lba in range(lba_start, lba_end):
                # map the LBA to a new value by ignoring specified bits 
                addr = mask_equiv_bits(cur_lba, bits)

                # mask the address where bits are ignored 
                hash_val = mmh3.hash128(str(addr), signed=False, seed=seed)

                # the timestamp to be used for samples generated if this block request is sampled 
                iat_ts = int(ts_tracker + row['iat'])
                if hash_val < limit:
                    # sample this block 
                    if cur_sample_block_req['size'] == 0:
                        # we were not tracking any sample block request 
                        # so start tracking  
                        if ts_method == 'iat':
                            cur_sample_block_req['ts'] = iat_ts
                        else:
                            cur_sample_block_req['ts'] = int(row['ts'])
                        
                        cur_sample_block_req['lba'] = cur_lba
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
                        row_json_list.append(copy.deepcopy(cur_sample_block_req))

                        # reset the sample block request 
                        cur_sample_block_req = {
                            'ts': 0,
                            'lba': 0,
                            'op': 'r',
                            'size': 0
                        }
            
            # we might exit while tracking a sample block request we never recorded 
            if cur_sample_block_req['size'] > 0:
                row_json_list.append(copy.deepcopy(cur_sample_block_req))
            
            # check if we generated any sample from this block request 
            if len(row_json_list) > prev_sample_count:
                split_counter[len(row_json_list) - prev_sample_count] += 1
                # update the timestamp for the next sample block request 
                ts_tracker += row['iat']
            
            # update sample count 
            prev_sample_count = len(row_json_list)
        
        sample_df = pd.DataFrame(row_json_list)
        end_time = time.time()
        process_time = end_time - start_time 
        print("prog->{} sampled in {:.5f} minutes".format(self.trace_path, float(process_time/60.0)))
        return sample_df, split_counter