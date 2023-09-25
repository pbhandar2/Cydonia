"""This script tests functions that algorithmically compute the new workload features
if all accesses to a block address are added or removed from a trace. 
"""

from pandas import read_csv, DataFrame 
from pathlib import Path 
from unittest import main, TestCase

from cydonia.sample.BlkSample import get_workload_stat_dict, get_percent_error_dict, blk_unsample, load_blk_trace


class TestBlkSample(TestCase):
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav/block_traces/cp/w66.csv")
    test_sample_block_trace_path = Path("/research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/w66/1_8_42.csv")

    test_data_dir = Path("../data/test_post_process")
    test_post_process_update_file_path = test_data_dir.joinpath("update.csv")
    test_post_process_metadata_file_path = test_data_dir.joinpath("metadata.json")
    test_post_process_sample_file_path = test_data_dir.joinpath("sample.json")

    def test_reduce_err_by_removing(self):
        full_df = load_blk_trace(self.test_block_trace_path)
        sample_df = load_blk_trace(self.test_sample_block_trace_path)
        workload_stat_dict = get_workload_stat_dict(full_df)
        sample_workload_stat_dict = get_workload_stat_dict(sample_df)
        cur_percent_error_dict = get_percent_error_dict(workload_stat_dict, sample_workload_stat_dict)
        print(cur_percent_error_dict)
        blk_unsample(sample_df, workload_stat_dict, num_lower_order_bits_ignored=24,
                        test_mode=True, test_trace_path=self.test_post_process_sample_file_path)


if __name__ == '__main__':
    main()