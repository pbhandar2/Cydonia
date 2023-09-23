"""This script tests functions that algorithmically compute the new workload features
if all accesses to a block address are added or removed from a trace. 
"""

from pandas import read_csv, DataFrame 
from pathlib import Path 
from unittest import main, TestCase

from copy import deepcopy 
from json import dumps, load 
from time import perf_counter

from cydonia.sample.SamplePP import SamplePP
from cydonia.sample.Sample import create_sample_trace
from cydonia.sample.BlkSample import add_block, BlkSample, remove_block, get_workload_stat_dict, eval_all_blk, get_percent_error_dict, get_remove_error_df, get_err_df_on_remove

from cydonia.sample.BlkSample import blk_unsample


class TestBlkSample(TestCase):
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav/block_traces/cp/w66.csv")
    test_sample_block_trace_path = Path("/research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/w66/1_8_42.csv")

    test_data_dir = Path("../data/test_post_process")
    test_post_process_update_file_path = test_data_dir.joinpath("update.csv")
    test_post_process_metadata_file_path = test_data_dir.joinpath("metadata.json")
    test_post_process_sample_file_path = test_data_dir.joinpath("sample.csv")


    def test_reduce_err_by_removing(self):
        full_df = SamplePP.load_block_trace(self.test_block_trace_path)
        sample_df = SamplePP.load_block_trace(self.test_sample_block_trace_path)
        per_blk_access_stat_dict = BlkSample.get_per_block_access_stat_dict(sample_df)
        workload_stat_dict = get_workload_stat_dict(full_df)
        sample_workload_stat_dict = get_workload_stat_dict(sample_df)
        cur_percent_error_dict = get_percent_error_dict(workload_stat_dict, sample_workload_stat_dict)
        print(cur_percent_error_dict)
        blk_unsample(sample_df, workload_stat_dict, num_lower_order_bits_ignored=12)


    def test_eval_all_blk(self):
        full_df = SamplePP.load_block_trace(self.test_block_trace_path)
        sample_df = SamplePP.load_block_trace(self.test_sample_block_trace_path)
        per_blk_access_stat_dict = BlkSample.get_per_block_access_stat_dict(sample_df)
        workload_stat_dict = get_workload_stat_dict(full_df)
        sample_workload_stat_dict = get_workload_stat_dict(sample_df)
        cur_percent_error_dict = get_percent_error_dict(workload_stat_dict, sample_workload_stat_dict)
        err_df = get_err_df_on_remove(sample_df, workload_stat_dict, num_lower_order_bits_ignored=12)



        

    def test_remove_block(self):
        sample_df = SamplePP.load_block_trace(self.test_sample_block_trace_path)
        per_blk_access_stat_dict = BlkSample.get_per_block_access_stat_dict(sample_df)
        workload_stat_dict = get_workload_stat_dict(sample_df)
        blk_addr_list = list(per_blk_access_stat_dict.keys())

        for blk_index, blk_addr in enumerate(blk_addr_list):
            print("Removing ", blk_addr, "{}/{}".format(blk_index, len(blk_addr_list)))
            workload_stat_dict = remove_block(workload_stat_dict, per_blk_access_stat_dict, blk_addr)

            sample_block_addr_dict = dict.fromkeys(per_blk_access_stat_dict, 1)
            create_sample_trace(sample_df, sample_block_addr_dict, self.test_post_process_sample_file_path)
            new_trace_df = BlkSample.load_block_trace(self.test_post_process_sample_file_path)
            new_trace_workload_stat_dict = SamplePP.get_workload_stat_dict(new_trace_df)

            for feature_key in new_trace_workload_stat_dict:
                assert new_trace_workload_stat_dict[feature_key] == workload_stat_dict[feature_key],\
                    "The feature {} in file and compute is not same {} vs {}.".format(feature_key, new_trace_workload_stat_dict, workload_stat_dict)


if __name__ == '__main__':
    main()