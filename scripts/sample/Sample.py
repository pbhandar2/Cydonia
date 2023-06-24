""" This class generates sample block traces from a block trace and sampling parameters. """

import os 
import pathlib 
import logging
import argparse 
import itertools
import numpy as np 
import pandas as pd 

from cydonia.util.S3Client import S3Client
from cydonia.sample.Sampler import Sampler

# constants / defaults 
SAMPLE_OUTPUT_DIR = pathlib.Path("/dev/shm")
METADATA_DIR = pathlib.Path("./data/sample_split")
MAX_BITS_IGNORE = 12
SAMPLE_RATE_LIST = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
PERCENTILE_TRACKED_ARRAY = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]


class Sample:
    def __init__(self, block_trace_path, output_dir):
        self.block_trace_path = block_trace_path 
        assert self.block_trace_path.exists(), "Block trace path {} does not exist".format(self.block_trace_path)

        # make sure the metadata and output directory for this block trace exists 
        self.workload_name = self.block_trace_path.stem 
        self.output_dir = output_dir.joinpath(self.workload_name)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.metadata_dir = METADATA_DIR.joinpath(self.workload_name)
        self.metadata_dir.mkdir(exist_ok=True, parents=True)

        # setup S3 client 
        self.aws_key = os.environ['AWS_KEY']
        self.aws_secret = os.environ['AWS_SECRET']
        self.aws_bucket = os.environ['AWS_BUCKET']
        self.s3 = S3Client(self.aws_key, self.aws_secret)

        self.sampler = Sampler(self.block_trace_path)
        self.default_bit_list = list(range(MAX_BITS_IGNORE))
        self.default_sample_rate_list = SAMPLE_RATE_LIST
        self.percentiles_array = PERCENTILE_TRACKED_ARRAY


    def generate_array_from_counter(self, counter):
        """ Get array filled with values represented in the counter 

            Parameters
            ----------
            counter : collections.Counter
                the counter of the number of split per block request 

            Return 
            ------
            array : np.array 
                array of values representing the frequency in the counter
        """
        total_items = sum(counter.values())
        array = np.zeros(total_items, dtype=int)

        cur_index = 0 
        for key in counter:
            array[cur_index:cur_index+counter[key]] = key
            cur_index += counter[key]
            
        return array 


    def get_stats_from_split_counter(self, split_counter, sample_rate, seed, bits, sample_file_path):
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
        total_request_sampled = sum(split_counter.values())
        stats = {}
        if total_request_sampled > 0:
            split_array = self.generate_array_from_counter(split_counter)

            stats['mean'] = np.mean(split_array) 
            stats['total'] = len(split_array)

            for index, percentile in enumerate(self.percentiles_array):
                stats['p_{}'.format(percentile)] = np.percentile(split_array, percentile, keepdims=False)
            
            no_split_count = len(split_array[split_array == 1])
            stats['freq%'] = int(np.ceil(100*(stats['total'] - no_split_count)/stats['total']))

            sample_df = pd.read_csv(sample_file_path, names=["ts", "lba", "op", "size"])
            stats['rate'] = sample_rate 
            stats['seed'] = seed 
            stats['bits'] = bits 
            stats['unique_lba_count'] = int(sample_df['lba'].nunique())
        else:
            stats = {
                'mean': 0,
                'total': 0,
                'freq%': 0,
                'rate': rate,
                'seed': seed,
                'bits': bits,
                'unique_lba_count': 0
            }
            for index, percentile in enumerate(self.percentiles_array):
                stats['p_{}'.format(percentile)] = 0
        return stats 
    
    
    def get_sample_file_name(self, sample_rate, bits, seed):
        """ Get the name of the sample file """
        return "{}_{}_{}_{}.csv".format(self.workload_name, int(sample_rate*100), seed, bits)


    def get_s3_key(self, ts_method, sample_rate, bits, seed):
        """ Get the S3 key to upload a sample based on its params """
        return "workloads/cp-{}/{}_{}_{}_{}.csv".format(ts_method, self.workload_name, int(sample_rate*100), seed, bits)
    

    def update_metadata(self, metadata_entry, metadata_file_name):
        """ Update the metadata file with an entry for the sample generated 
            
            Parameters
            ----------
            metadata_entry : dict 
                a dictionary representing a row in the metadata file 
        """
        metadata_file_path = self.metadata_dir.joinpath("{}.csv".format(metadata_file_name)
        df = pd.DataFrame([metadata_entry])
        if metadata_file_path.exists():
            df.to_csv(), mode='a+', index=False)
        else:
            df.to_csv(), mode='a+', index=False, header=False)
    

    def sample(self, ts_method, sample_rate_list, bit_list, seed):
        """  Generate sample block traces based on the parameters provided

            Parameters
            ----------
            ts_method : str 
                the method to generate timestamps in the sample 
            sample_rate_list : list(float)
                list of sampling rates
            bit_list : list(int)
                list of lower order bits to ignore 
            seed : int 
                random seed 
        """
        if bit_list is None:
            bit_list = self.default_bit_list

        if sample_rate_list is None:
            sample_rate_list = self.default_sample_rate_list 

        for sample_rate, bits in itertools.product(*[sample_rate_list, bit_list]):
            s3_key = self.get_s3_key(ts_method, sample_rate, bits, seed)

            # check if s3 key exists if it does, move to the next one 
            if self.s3.check_prefix_exist(s3_key):
                print("Sample in S3 {} already!".format(s3_key))
                continue 
            
            sample_file_name = self.get_sample_file_name(sample_rate, bits, seed)
            sample_file_path = self.output_dir.joinpath(sample_file_name)
            sample_file_path.touch(exist_ok=True)

            # upload a tmp file to S3 to lock this sample to prevent or machine from generating the same sample 
            self.s3.upload_s3_obj(s3_key, str(sample_file_path.absolute()))

            print("Running sampling for s3 key: {}".format(s3_key))
            split_counter = self.sampler.sample(sample_rate, seed, bits, ts_method, sample_file_path)
            split_stats = self.get_stats_from_split_counter(split_counter, sample_file_path)

            # update metadata stats to a file named after the ts_method 
            self.update_metadata(split_stats, ts_method)

            # upload the sample to S3 
            self.s3.upload_s3_obj(s3_key, str(sample_file_path.absolute()))
            print("Sample {} uploaded".format(s3_key))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate sample block traces from a block trace",
        epilog="Example usage: python3 Sample.py ~/w20.csv /dev/shm --rate 0.01 0.5 --bits 0 1 2")

    parser.add_argument("block_trace_path", 
        type=pathlib.Path,
        help="Path to the block trace to be sampled")
    
    parser.add_argument("output_dir",
        type=pathlib.Path,
        help="Directory to store the sample block traces generated")
    
    parser.add_argument("--ts", 
        default="iat", 
        choices=["iat", "ts"],
        help="Method to generate timestamps (Default: 'ts')")

    parser.add_argument("--rate",
        nargs="*",
        type=float,
        default=None,
        help="The list of sample rates to evaluate")

    parser.add_argument("--bits",
        nargs="*",
        type=str,
        default=None,
        help="List of different number of lower order bits to ignore")
    
    parser.add_argument("--seed",
        type=int,
        default=42,
        help="Random seed")

    args = parser.parse_args()

    sampler = Sample(args.block_trace_path, args.output_dir)
    sampler.sample(args.ts, args.rate, args.bits, args.seed)