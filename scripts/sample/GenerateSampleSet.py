""" This scripts generates a set of samples for a given workload. 
"""

import argparse 
import pathlib 
import itertools 
import numpy as np 
import pandas as pd 

from cydonia.sample.Sampler import Sampler 


BLOCK_TRACE_DIR = pathlib.Path("/research2/mtc/cp_traces/csv_traces")
SAMPLE_TRACE_DIR = pathlib.Path("/research2/mtc/cp_traces/sample/block")


class SampleSet:
    def __init__(self, workload_name, ts_method, trace_dir, sample_dir, rate_list):
        self.trace_dir = trace_dir
        self.bits_list = [None, 
                            range(3), # ignore bits 0,1 and 2 
                            range(0,3,2),
                            range(6), 
                            range(0,6,2), # ignore bits 0, 2 and 4
                            range(0,6,3), 
                            range(9),
                            range(0,9,2),
                            range(0,9,3),
                            range(12),
                            range(0,12,2),
                            range(0,12,3),
                            range(15),
                            range(0,15,2),
                            range(0,15,3)]
        if rate_list is None:
            self.sample_rate_list = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        else:
            self.sample_rate_list = rate_list
        self.seed_list = [42, 43, 44]
        self.percentiles_array = np.array([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100])

        self.ts_method = ts_method
        self.workload_name = workload_name 
        self.trace_path = self.trace_dir.joinpath("{}.csv".format(self.workload_name))
        self.sampler = Sampler(self.trace_path)
        self.sample_dir = sample_dir.joinpath(self.workload_name)
        self.sample_dir.mkdir(exist_ok=True)
        self.sample_output_data_path = pathlib.Path("./data/sample_split/{}.csv".format(self.workload_name))
    

    def get_bit_str(self, bits):
        if bits is None:
            return 'na'
        else:
            return "-".join([str(_) for _ in bits])
    

    def generate_array_from_counter(self, counter):
        total_items = sum(counter.values())
        array = np.zeros(total_items, dtype=int)

        cur_index = 0 
        for key in counter:
            array[cur_index:cur_index+counter[key]] = key
            cur_index += counter[key]
            
        return array 
    

    def get_stats_from_split_counter(self, split_counter):
        """ Get statistics (mean, min, max, percentiles) from the counter of number of samples 
            generated from a sampled block request. 

            Parameters
            ----------
            split_counter : collections.Counter 
                a Counter of the number of samples generated per sampled block request 

            Return 
            ------
            stats : dict 
                get split statistics such as (mean, min, max, percentiles) from the split_counter
        """
        split_array = self.generate_array_from_counter(split_counter)

        stats = {}
        stats['mean'] = np.mean(split_array)
        stats['total'] = len(split_array)

        for index, percentile in enumerate(self.percentiles_array):
            stats['p_{}'.format(percentile)] = np.percentile(split_array, percentile, keepdims=False)
        
        no_split_count = len(split_array[split_array == 1])
        stats['freq%'] = int(np.ceil(100*(stats['total'] - no_split_count)/stats['total']))
        
        return stats 


    def generate(self):
        sample_output_data = []
        param_list = [self.seed_list, self.sample_rate_list, self.bits_list]

        for seed, sample_rate, bits in itertools.product(*param_list):   
            bit_str = self.get_bit_str(bits)

            sample_file_name = "{}_{}_{}_{}.csv".format(self.ts_method, int(sample_rate*100), seed, bit_str)
            sample_path = self.sample_dir.joinpath(sample_file_name)

            if sample_path.exists():
                print("Sample already exists! {}".format(sample_path))
                continue 

            print("Sampling->rate={},seed={},bits={},workload={}".format(sample_rate, seed, bit_str, self.workload_name))
            sample_df, split_count_dict = self.sampler.sample(sample_rate, 
                                                                seed, 
                                                                bits, 
                                                                self.ts_method)

            split_stats = self.get_stats_from_split_counter(split_count_dict)
            split_stats['rate'] = sample_rate 
            split_stats['seed'] = seed 
            split_stats['bits'] = bit_str 
            sample_output_data.append(split_stats)

            sample_df = sample_df.astype({"ts": int, "lba": int, "op": str, "size": int})
            sample_df.to_csv(sample_path, index=False, header=False)

            cur_df = pd.DataFrame(sample_output_data)
            print(cur_df.to_string())

        cur_df = pd.DataFrame(sample_output_data)
        cur_df.to_csv(self.sample_output_data_path, index=False)

                
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a set of samples for a given workload")
    
    parser.add_argument("workload_name", help="Name of the workload")

    parser.add_argument("--ts", default="iat", help="Method to generate timestamps 'iat' or 'ts'")

    parser.add_argument("--block_trace_dir", 
                            default=BLOCK_TRACE_DIR, 
                            type=pathlib.Path, 
                            help="Directory containing block traces")

    parser.add_argument("--sample_trace_dir", 
                            default=SAMPLE_TRACE_DIR,
                            type=pathlib.Path, 
                            help="Directory to output the sample trace")
    
    parser.add_argument("--rate",
                            nargs="*",
                            type=float,
                            default=None,
                            help="The sample rates to generate")

    args = parser.parse_args()

    sample_generator = SampleSet(args.workload_name, args.ts, args.block_trace_dir, args.sample_trace_dir, args.rate)
    sample_generator.generate()