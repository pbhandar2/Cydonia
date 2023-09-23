from pathlib import Path 
from copy import deepcopy 
from unittest import main, TestCase
from pandas import read_csv 
from json import dumps, load 

from cydonia.sample.SamplePP import SamplePP
from cydonia.sample.Sample import create_sample_trace


class TestSamplePP(TestCase):
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav/block_traces/test/w66.csv")
    test_sample_block_trace_path = Path("/research2/mtc/cp_traces/pranav/sample_block_traces/iat/test/w66/20_8_42.csv")

    test_data_dir = Path("../data/test_post_process")
    test_post_process_update_file_path = test_data_dir.joinpath("update.csv")
    test_post_process_metadata_file_path = test_data_dir.joinpath("metadata.json")
    test_post_process_sample_file_path = test_data_dir.joinpath("sample.csv")


    def test_get_error_df(self):
        sample_pp = SamplePP(self.test_block_trace_path, self.test_sample_block_trace_path)
        sample_pp.post_process(self.test_post_process_metadata_file_path, type="mean")

    
    def test_get_per_region_error_df(self):
        sample_pp = SamplePP(self.test_block_trace_path, self.test_sample_block_trace_path)
        new_workload_stat_dict = deepcopy(sample_pp._sample_workload_dict)
        access_stat_dict = deepcopy(sample_pp._access_stat)
        print(new_workload_stat_dict)
        block_list = list(access_stat_dict.keys())
        for block_addr in block_list:
            print(block_addr)
            print(new_workload_stat_dict)
            access_feature_dict = access_stat_dict[block_addr]
            print(access_feature_dict)

            left_sampled = block_addr - 1 in access_stat_dict
            right_sampled = block_addr + 1 in access_stat_dict
            new_sub_remove_dict = sample_pp.get_new_sub_remove_dict(access_feature_dict, left_sampled, right_sampled)
            print(new_sub_remove_dict)

            new_workload_stat_dict = sample_pp.update_workload(new_workload_stat_dict, new_sub_remove_dict)
            sample_pp.remove_block(access_stat_dict, block_addr)
            assert block_addr not in access_stat_dict
            print(new_workload_stat_dict)

            create_sample_trace(sample_pp._sample_df, dict.fromkeys(access_stat_dict.keys(), 1), self.test_post_process_sample_file_path)
            sample_df = sample_pp.load_block_trace(self.test_post_process_sample_file_path)
            file_trace_stat_dict = sample_pp.get_workload_stat_dict(sample_df)
            print(file_trace_stat_dict)

            
            for feature_key in file_trace_stat_dict:
                assert file_trace_stat_dict[feature_key] == new_workload_stat_dict[feature_key],\
                    "The feature {} in file and compute is not same {} vs {}.".format(feature_key, file_trace_stat_dict, new_workload_stat_dict)


if __name__ == '__main__':
    main()