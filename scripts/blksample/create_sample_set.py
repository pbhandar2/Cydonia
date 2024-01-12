from argparse import ArgumentParser

from keyuri.config.BaseConfig import BaseConfig
from cydonia.blksample.sample import random_spatial_sampling
from cydonia.profiler.CacheTraceProfiler import load_cache_trace


def main():
    parser = ArgumentParser(description="Use BlkSample algorithm to reduce sample to improve feature accuracy.")
    parser.add_argument("workload", type=str, help="Name of the workload.")
    parser.add_argument("num_lower_order_bits_ignored", type=int, help="Number of lower order bits of block addresses that are ignored.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    sample_type = "eff"
    config = BaseConfig()
    sample_output_dir_path = config.get_sample_cache_trace_dir_path(sample_type, args.workload)
    sample_output_dir_path.mkdir(exist_ok=True, parents=True)
    full_cache_trace_path = config.get_cache_trace_path(args.workload)
    cache_trace_df = load_cache_trace(full_cache_trace_path)
    random_spatial_sampling(cache_trace_df, args.num_lower_order_bits_ignored, args.seed, sample_output_dir_path)


if __name__ == "__main__":
    main()