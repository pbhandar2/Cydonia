from pathlib import Path 
from unittest import main, TestCase
from pandas import read_csv 
from json import load 

from cydonia.blksample.SamplePP import SamplePP
from cydonia.profiler.CacheTraceProfiler import get_workload_features_from_cache_trace
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap


class TestSamplePP(TestCase):
    test_sample_cache_trace_path = Path("/research2/mtc/cp_traces/pranav-phd/cp--iat/cache_traces/w66/1_4_42.csv")
    test_full_cache_feature_path = Path("/research2/mtc/cp_traces/pranav-phd/cp/cache_features/w66.csv")
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav-phd/cp/block_traces/w66.csv")
    test_sample_block_trace_path = Path("/research2/mtc/cp_traces/pranav-phd/cp--iat/block_traces/w66/1_4_42.csv")

    def test_get_error_df(self):
        with self.test_full_cache_feature_path.open("r") as cache_feature_handle:
            full_cache_feature_dict = load(cache_feature_handle)
        
        num_bits = 0 
        block_size_byte = int((2**num_bits) * 4096)
        
        feature_map = BlockAccessFeatureMap(block_size_byte=block_size_byte)
        feature_map.load(self.test_sample_block_trace_path)

        cache_trace_df = read_csv(self.test_sample_cache_trace_path, 
                                    names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"],
                                    dtype={"i": int, "iat": int, "key": int, "op": str, "front_misalign": int, "rear_misalign": int })
        
        sample_cache_feature_dict = get_workload_features_from_cache_trace(cache_trace_df)
        sample_pp = SamplePP(sample_cache_feature_dict, full_cache_feature_dict, feature_map)
        block_removed = sample_pp.remove_next_block()

        while block_removed >= 0:
            cache_trace_df = cache_trace_df[~cache_trace_df["key"].isin([block_removed])]
            sample_cache_feature_dict = get_workload_features_from_cache_trace(cache_trace_df)

            for feature_name in sample_cache_feature_dict:
                file_value = sample_cache_feature_dict[feature_name]
                compute_value = sample_pp._cur_workload_feature_dict[feature_name]
                assert  file_value == compute_value,\
                    "Feature ({}) value not equal in file {} and compute {}. \n {} \n {}".format(feature_name, 
                                                                                                    file_value, 
                                                                                                    compute_value,
                                                                                                    sample_cache_feature_dict,
                                                                                                    sample_pp._cur_workload_feature_dict)

            block_removed = sample_pp.remove_next_block()


if __name__ == '__main__':
    main()