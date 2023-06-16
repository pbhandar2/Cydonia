""" This class generates samples from a block storage traces based on user specified params. 
"""

import pathlib 
import time
import copy 
import mmh3
import pandas as pd 
import numpy as np 

from cydonia.sample.util import mask_equiv_bits


class Sampler:
    def __init__(self, trace_path, bits=None, sector_size=512):
        self.trace_path = pathlib.Path(trace_path)

        self.bits = bits 
        self.sector_size = sector_size
        self.max_hash_val = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF 

        self.df = pd.read_csv(self.trace_path, names=['ts', 'lba', 'op', 'size'])
        self.df['iat'] = self.df["ts"].diff()


    def sample(self, rate, seed, ts_method):
        """ Sample the block trace using the specified rate, random seed and method for generating 
            timestamps. 

            Parameters
            ----------
            rate (float) : sampling rate 
            seed (int) : random seed 
            ts_method (str) : method of timestamp generation ('iat' or 'ts')

            Return 
            ------
            sample_df (pd.DataFrame) : the DataFrame containing the sample block trace 
            sampled_block_req_count (int) : the number of block requests in the original trace that had sampled blocks
        """
        start_time = time.time()

        limit = self.max_hash_val * rate 

        # number of block requests that had blocks that were sampled 
        sampled_block_req_count = 0 
        prev_num_sample_req = 0

        row_json_list = []
        for row_index, row in self.df.iterrows():
            lba_start = row['lba']
            lba_end = lba_start + int(row['size']/self.sector_size)

            ts_tracker = 0 
            cur_block_req = {
                'ts': 0,
                'lba': 0,
                'op': 'r',
                'size': 0
            }
 
            if row_index % 1e6 == 0:
                end_time = time.time()
                process_time = end_time - start_time 
                print("prog->{} processed in {:.5f} minutes".format(row_index, float(process_time/60.0)))

            for cur_lba in range(lba_start, lba_end):
                addr = mask_equiv_bits(cur_lba, self.bits)
                hash_val = mmh3.hash128(str(addr), signed=False, seed=seed)
                if hash_val < limit:
                    # sample this adr
                    if cur_block_req['size'] == 0:
                        # we were not tracking any block request so lets start 
                        if ts_method == 'iat':
                            cur_block_req['ts'] = ts_tracker + row['iat']
                        else:
                            cur_block_req['ts'] = row['ts']
                        
                        cur_block_req['lba'] = cur_lba
                        cur_block_req['op'] = row['op']
                        cur_block_req['size'] = self.sector_size
                    else:
                        cur_block_req['size'] += self.sector_size
                else:
                    # dont sample this addr 
                    if cur_block_req['size'] > 0:
                        # this is a block request in sample
                        row_json_list.append(copy.deepcopy(cur_block_req))

                        # reset the block req
                        cur_block_req = {
                            'ts': 0,
                            'lba': 0,
                            'op': 'r',
                            'size': 0
                        }
        
            if cur_block_req['size'] > 0:
                # this is a block request in sample
                row_json_list.append(copy.deepcopy(cur_block_req))
            
            if len(row_json_list) > prev_num_sample_req:
                sampled_block_req_count += 1
            
            prev_num_sample_req = len(row_json_list)
            ts_tracker += row['iat']
        
        sample_df = pd.DataFrame(row_json_list)
        end_time = time.time()
        process_time = end_time - start_time 

        print("prog->{} sampled in {:.5f} minutes".format(self.trace_path, float(process_time/60.0)))
        return sample_df, sampled_block_req_count