from pathlib import Path 
from json import dumps, JSONEncoder
from numpy import ndarray
from unittest import main, TestCase
from pandas import read_csv 

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.CacheTraceProfiler import get_workload_features_from_cache_trace
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)


class TestBlockAccessFeatureMap(TestCase):
    def test_access_feature_map(self):
        test_block_trace_path = Path("../data/test_cp.csv")
        test_cache_trace_path = Path("../data/test_cp_cache.csv")

        cache_trace_df = read_csv(test_cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        first_cache_req_key = cache_trace_df.iloc[0]["key"]

        feature_map = BlockAccessFeatureMap()
        feature_map.load(test_block_trace_path, 0)

        print("Creating temp cache trace {} from block trace {}.".format(test_cache_trace_path, test_block_trace_path))
        reader = CPReader(test_block_trace_path)
        reader.generate_cache_trace(test_cache_trace_path)
        reader.reset()
        reader.trace_file_handle.close()

        workload_feature_dict = get_workload_features_from_cache_trace(cache_trace_df)
        filtered_df = cache_trace_df[~cache_trace_df["key"].isin([first_cache_req_key])]

        new_workload_feature_dict = feature_map.get_workload_feature_dict_on_removal(workload_feature_dict, first_cache_req_key)
        filtered_new_workload_feature_dict = get_workload_features_from_cache_trace(filtered_df)
        for feature_name in new_workload_feature_dict:
            print("{} -> Compute = {}, File = {}, OLD = {}".format(feature_name, new_workload_feature_dict[feature_name], filtered_new_workload_feature_dict[feature_name], workload_feature_dict[feature_name]))


if __name__ == '__main__':
    main()