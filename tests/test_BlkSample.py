"""This script tests functions that algorithmically compute the new workload features
if all accesses to a block address are added or removed from a trace. 
"""

from pandas import read_csv, DataFrame 
from pathlib import Path 
from unittest import main, TestCase

from cydonia.blksample.blksample import blksample, get_workload_feature_dict_from_block_trace, get_feature_err_dict
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap


class TestBlkSample(TestCase):
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav-phd/cp/block_traces/w105.csv")
    test_sample_block_trace_path = Path("/research2/mtc/cp_traces/pranav-phd/cp--iat/block_traces/w105/5_0_44.csv")
    test_per_iteration_output_path = Path("../data/temp_per_iteration_output.csv")

    def test_reduce_err_by_removing(self):
        feature_map = BlockAccessFeatureMap()
        feature_map.load(self.test_sample_block_trace_path, 4)
        print("Feature map computed...")

        full_workload_feature_dict = get_workload_feature_dict_from_block_trace(self.test_block_trace_path)
        print(full_workload_feature_dict)
        sample_workload_feature_dict = get_workload_feature_dict_from_block_trace(self.test_sample_block_trace_path)
        print(sample_workload_feature_dict)

        feature_err_dict = get_feature_err_dict(full_workload_feature_dict, sample_workload_feature_dict)
        print(feature_err_dict)

        blksample(feature_map, full_workload_feature_dict, sample_workload_feature_dict, self.test_per_iteration_output_path)

        

if __name__ == '__main__':
    main()