""" This script generates sample block traces. """

import argparse 
import pathlib 
import pandas as pd 

import logging
import logging.handlers as handlers

from cydonia.sample.Sampler import Sampler


logger = logging.getLogger('sample_logger')
logger.setLevel(logging.INFO)
logHandler = handlers.RotatingFileHandler('/dev/shm/sample.log', maxBytes=25*1e6)
logHandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


BITS_ARR = [0, 2, 4, 6, 8, 10, 12]
SEED_ARR = [42, 43, 44]
RATE_ARR = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.9]
PERCENTILE_TRACKED_ARRAY = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]
TS_ARR = ['iat', 'iat0', 'iatscale']


def generate_array_from_counter(counter):
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


def get_stats_from_split_counter(split_counter, rate, seed, bits, sample_path):
    """ Get statistics (mean, min, max, percentiles) from the counter of number of samples 
        generated from a sampled block request. 

        Parameters
        ----------
        split_counter : collections.Counter 
            a Counter of the number of samples generated per sampled block request 
        
        rate : float 
            sampling rate 
        
        seed : int 
            random seed 
        
        bits : int 
            number of lower order bits to ignore 
        
        sample_path : pathlib.Path 
            path to output the sample 

        Return 
        ------
        stats : dict 
            get split statistics such as (mean, min, max, percentiles) from the split_counter
    """
    total_request_sampled = sum(split_counter.values())
    stats = {}
    if total_request_sampled > 0:
        split_array = generate_array_from_counter(split_counter)

        stats['mean'] = np.mean(split_array) 
        stats['total'] = len(split_array)

        for index, percentile in enumerate(PERCENTILE_TRACKED_ARRAY):
            stats['p_{}'.format(percentile)] = np.percentile(split_array, percentile, keepdims=False)
        
        no_split_count = len(split_array[split_array == 1])
        stats['freq%'] = int(np.ceil(100*(stats['total'] - no_split_count)/stats['total']))

        sample_df = pd.read_csv(sample_path, names=["ts", "lba", "op", "size"])
        stats['rate'] = rate 
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
        for index, percentile in enumerate(PERCENTILE_TRACKED_ARRAY):
            stats['p_{}'.format(percentile)] = 0
    return stats 


def main(args):
    """ Iterate through each block trace to sample it and update its metadata """
    logging.info("Init: {}".format(args))
    for block_trace_path in args.block_trace_dir.iterdir():
        workload_name = block_trace_path.stem 
        if args.workload_filter is not None:
            if workload_name not in args.workload_filter:
                continue 
        
        sampler = Sampler(block_trace_path)
        for rate, seed, bits, ts in zip(args.rate, args.bits, args.seed, args.ts):
            sample_dir = args.sample_trace_dir.joinpath(ts, workload_name)
            sample_dir.mkdir(exist_ok=True, parents=True)
            sample_file_name = "{}_{}_{}.csv".format(rate, seed, bits)
            sample_path = sample_dir.joinpath(sample_file_name)
            if sample_path.exists():
                logging.info("Done: {}".format(sample_path))
                continue 
            
            split_counter = sampler.sample(rate, seed, bits, ts, sample_path)
            split_stats = get_stats_from_split_counter(split_counter, rate, seed, bits, sample_path)
            metadata_dir = args.metadata_dir.joinpath(ts, workload_name)
            metadata_dir.mkdir(exist_ok=True, parents=True)
            metadata_file_path = metadata_dir.joinpath("split.csv")
            metadata_df = pd.DataFrame([split_stats])
            if metadata_file_path.exists():
                current_metadata_df = pd.read_csv(metadata_file_path)
                updated_metadata_df = pd.concat([current_metadata_df, metadata_df], ignore_index=True)
                updated_metadata_df.to_csv(metadata_file_path, index=False)
            else:
                metadata_df.to_csv(metadata_file_path, index=False)
            
            logging.info("Processed: {}".format(sample_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genrate sample block traces given a directory of block traces",
                formatter_class=argparse.RawDescriptionHelpFormatter,
                epilog="Notes:\n"
                        "* Example usage: python3 sample.py /home/block_trace_dir /home/sample_dir")
    
    parser.add_argument("block_trace_dir",
        type=pathlib.Path,
        help="Directory containing block storage traces")
    
    parser.add_argument("sample_trace_dir",
        type=pathlib.Path,
        help="Directory where the samples are generated")

    parser.add_argument("metadata_dir",
        type=pathlib.Path,
        help="Directory of sample metadata")
    
    parser.add_argument("--rate",
        nargs="*",
        type=float,
        default=RATE_ARR,
        help="Array of sample rates (default: {})".format(RATE_ARR))

    parser.add_argument("--bits",
        nargs="*",
        type=int,
        default=BITS_ARR,
        help="Array of number of lower order address bits to ignore when sampling (default: {})".format(BITS_ARR))

    parser.add_argument("--seed",
        nargs="*",
        type=int,
        default=SEED_ARR,
        help="Array of random seed (default: {})".format(SEED_ARR))

    parser.add_argument("--ts", 
        nargs="*",
        type=str, 
        choices=TS_ARR,
        default=TS_ARR,
        help="Array of methods to generate timestamps (default: {})".format(TS_ARR))
    
    parser.add_argument("--workload_filter",
        nargs = "+",
        type = str,
        default = None, 
        help="Select block traces to process based on the names of the filter (if filter is 'w20' only 'w20.csv' will be processed)")

    args = parser.parse_args()

    main(args)