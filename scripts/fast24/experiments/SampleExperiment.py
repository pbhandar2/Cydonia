""" This class generates experiment files to replay block trace samples. """

import json 
import pandas as pd 
from argparse import ArgumentParser
from pathlib import Path 
from copy import deepcopy 
from itertools import product 
from numpy import ceil 


class SampleExperiment:
    def __init__(self) -> None:
        """Sample experiment generates an experiment file to replay block trace samples. 

        Attributes:
            bits_arr: Array of number of lower order bits of block addresses we ignore. 
            sample_rate_array: Array of sample rates. 
        """
        self.bits_arr = [12, 8, 4, 0]
        self.sample_rate_arr = [0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8]
        self.seed = 42
        self.replay_rate_arr =[2, 1]
        self.sample_dir = Path("/research2/mtc/cp_traces/pranav/sample/iat/")
        self.sample_dir.mkdir(exist_ok=True, parents=True)

    
    def get_experiment_list(
        self, 
        cache_size_mb: int, 
        max_cache_size_mb: int, 
        workload_type: str, 
        sample_type: str, 
        workload: str, 
        replay_rate: int 
    ) -> list:
        experiment_list = []

        # Replay an ST and MT configuration with the full trace first 
        base_config = {
            "t1_size_mb": cache_size_mb,
            "trace_s3_key": "blocktrace/{}/{}.csv".format(workload_type, workload),
            "kwargs": {
                "replayRate": replay_rate
            }
        }
        st_config = deepcopy(base_config)
        experiment_list.append(st_config)
        if (max_cache_size_mb - cache_size_mb) >= 150:
            mt_config = deepcopy(base_config)
            mt_config["kwargs"]["nvmCacheSizeMB"] = max_cache_size_mb - cache_size_mb
            experiment_list.append(mt_config)
        
        # Now replay samples based on scaled down caches 
        for bits, rate in product(self.bits_arr, self.sample_rate_arr):
            scaled_cache_size_mb = int(rate * cache_size_mb)

            if scaled_cache_size_mb < 100:
                continue 
            
            sample_st_config = deepcopy(base_config)
            sample_st_config["t1_size_mb"] = scaled_cache_size_mb
            sample_st_config["trace_s3_key"] = "blocktrace/sample/{}/{}/{}/{}_{}_{}.csv".format(workload_type, sample_type, workload, rate, bits, self.seed)
            experiment_list.append(sample_st_config)

            scaled_nvm_size_mb = int(rate * (max_cache_size_mb - cache_size_mb))
            if scaled_nvm_size_mb >= 150:
                sample_mt_config = deepcopy(sample_st_config)
                sample_mt_config["kwargs"]["nvmCacheSizeMB"] = scaled_nvm_size_mb
                experiment_list.append(sample_mt_config)
        
        return experiment_list 
            
   
    def generate(
        self, 
        cache_stat_file_path: str,
        workload_type: str, 
        sample_type: str, 
        workload: str
    ) -> None:
        with open(cache_stat_file_path) as f:
            cache_stat = json.load(f)

        max_cache_size_mb = (cache_stat['size_100']//256) + 1
        
        final_list = []
        for replay_rate in self.replay_rate_arr:
            for wss_percent in range(10,101,10):
                cache_size = cache_stat['size_{}'.format(wss_percent)]
                cache_size_mb = cache_size//256

                if cache_size_mb < 100:
                    continue 

                experiment_list = self.get_experiment_list(cache_size_mb, max_cache_size_mb, workload_type, sample_type, workload, replay_rate)
                final_list += experiment_list 
        
        output_file_path = "files/sample/{}_{}_{}.json".format(workload_type, sample_type, workload)
        with open(output_file_path, "w+") as f:
            f.write(json.dumps(final_list, indent=4))


def main(args):
    sample_experiment_generator = SampleExperiment()
    sample_experiment_generator.generate(args.cache_stat_file_path, args.workload_type, args.sample_technique, args.workload_name)


if __name__ == "__main__":
    parser = ArgumentParser(description="Generate experiment files to replay block trace samples.")
    parser.add_argument("cache_stat_file_path",
        default=None,
        help="Path to file containig cache statistics of the trace.")
    parser.add_argument("workload_type",
        help="Type of workload.")
    parser.add_argument("sample_technique",
        help="Sampling technique used.")
    parser.add_argument("workload_name",
        help="Name used to generate sample file names.")
    args = parser.parse_args()
    main(args)