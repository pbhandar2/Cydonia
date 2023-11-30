from pathlib import Path 
from pandas import read_csv
from unittest import main, TestCase

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockTraceProfiler import BlockTraceProfiler
from cydonia.profiler.CacheTraceProfiler import get_workload_features_from_cache_trace, generate_block_trace, eval_all_blocks, basic_algo
from cydonia.blksample.sample import get_sampled_blocks, generate_sample_cache_trace

class TestBlockTraceProfiler(TestCase):
    def test_block_and_cache_trace_profiler(self):
        test_block_trace_path = Path("../data/test_cp.csv")
        test_cache_trace_path = Path("../data/test_cp_cache.csv")

        print("Creating temp cache trace {} from block trace {}.".format(test_cache_trace_path, test_block_trace_path))
        reader = CPReader(test_block_trace_path)
        reader.generate_cache_trace(test_cache_trace_path)
        reader.reset()

        block_trace_profiler = BlockTraceProfiler(reader)
        block_trace_profiler.run()
        block_workload_stat_dict = block_trace_profiler.get_stat()
        reader.trace_file_handle.close()

        test_cache_trace_df = read_csv(test_cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        cache_workload_stat_dict = get_workload_features_from_cache_trace(test_cache_trace_df)
        for feature_name in cache_workload_stat_dict:
            print("{} -> Cache = {}, Block = {}".format(feature_name, cache_workload_stat_dict[feature_name], block_workload_stat_dict[feature_name]))
            assert cache_workload_stat_dict[feature_name] == block_workload_stat_dict[feature_name], \
                "{} -> Cache = {}, Block = {}".format(feature_name, cache_workload_stat_dict[feature_name], block_workload_stat_dict[feature_name])
        
        test_cache_trace_path.unlink()
    

    def test_sample_and_profile(self):
        test_block_trace_path = Path("../data/test_cp.csv")
        test_cache_trace_path = Path("../data/test_cp_cache.csv")
        test_sample_block_trace_path = Path("../data/sample_test_cp_block.csv")
        test_sample_cache_trace_path = Path("../data/sample_test_cp_cache.csv")
        
        print("Creating temp cache trace {} from block trace {}.".format(test_cache_trace_path, test_block_trace_path))
        reader = CPReader(test_block_trace_path)
        reader.generate_cache_trace(test_cache_trace_path)
        reader.reset()
        reader.trace_file_handle.close()

        full_cache_trace_df = read_csv(test_cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        full_cache_workload_stat_dict = get_workload_features_from_cache_trace(full_cache_trace_df)

        sample_dict = get_sampled_blocks(test_cache_trace_path, 0.2, 42, 0)
        generate_sample_cache_trace(test_cache_trace_path, sample_dict, test_sample_cache_trace_path)

        test_sample_cache_trace_df = read_csv(test_sample_cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        cache_workload_stat_dict = get_workload_features_from_cache_trace(test_sample_cache_trace_df)

        generate_block_trace(test_sample_cache_trace_path, test_sample_block_trace_path)
        reader = CPReader(test_sample_block_trace_path)
        block_trace_profiler = BlockTraceProfiler(reader)
        block_trace_profiler.run()
        block_workload_stat_dict = block_trace_profiler.get_stat()
        reader.trace_file_handle.close()

        for feature_name in cache_workload_stat_dict:
            print("{} -> Cache = {}, Block = {}".format(feature_name, cache_workload_stat_dict[feature_name], block_workload_stat_dict[feature_name]))
            assert cache_workload_stat_dict[feature_name] == block_workload_stat_dict[feature_name], \
                "{} -> Cache = {}, Block = {}".format(feature_name, cache_workload_stat_dict[feature_name], block_workload_stat_dict[feature_name])


        basic_algo(test_sample_cache_trace_df, full_cache_workload_stat_dict, 0)
        basic_algo(test_sample_cache_trace_df, full_cache_workload_stat_dict, 1)
        basic_algo(test_sample_cache_trace_df, full_cache_workload_stat_dict, 2)

        test_cache_trace_path.unlink()
        test_sample_block_trace_path.unlink()
        test_sample_cache_trace_path.unlink()

    





if __name__ == '__main__':
    main()