from importlib import metadata
from multiprocessing import process
from pathlib import Path 
from unittest import main, TestCase
from cydonia.sample.PostProcess import PostProcess
from pandas import read_csv 
from json import dumps, load 
from copy import deepcopy 

from cydonia.sample.Sample import create_sample_trace


class TestSamplePostProcess(TestCase):
    test_block_trace_path = Path("/research2/mtc/cp_traces/pranav/block_traces/test/w66.csv")
    test_sample_block_dir_path = Path("/research2/mtc/cp_traces/pranav/sample_block_traces/iat/test/w66/5_0_42.csv")

    test_data_dir = Path("../data/test_post_process")
    test_post_process_update_file_path = test_data_dir.joinpath("update.csv")
    test_post_process_metadata_file_path = test_data_dir.joinpath("metadata.json")
    test_post_process_sample_file_path = test_data_dir.joinpath("sample.csv")

    
    def test_get_per_region_error_df(self):
        print("Testing get_per_region_error_df")
        num_lower_order_bits_ignored = 6
        processor = PostProcess(str(self.test_block_trace_path.expanduser()), str(self.test_sample_block_dir_path.expanduser()))

        per_unsampled_block_dict = processor.per_unsampled_block_stat
        block_sample_dict = processor._sample_lba_dict
        sample_workload_stat_dict = processor._sample_stat_dict
        full_workload_stat_dict = processor._full_stat_dict
        per_region_error_df = processor.get_per_region_error_df(per_unsampled_block_dict, num_lower_order_bits_ignored, block_sample_dict, 
                                                                    sample_workload_stat_dict, full_workload_stat_dict)

        best_region_entry = per_region_error_df[per_region_error_df['mean']==per_region_error_df['mean'].min()]
        best_block = int(best_region_entry["block"])

        sample_lba_dict = deepcopy(processor._sample_lba_dict)
        trace_stat_dict = deepcopy(processor._sample_stat_dict)
        region_block_arr = processor.get_block_addr_arr(best_block, num_lower_order_bits_ignored)
        for region_block_addr in region_block_arr:
            if region_block_addr not in processor.per_unsampled_block_stat:
                continue 
            lba_access_dict = processor.per_unsampled_block_stat[region_block_addr]
            metric_dict, new_trace_stat_dict = processor.compute_priority_metrics(region_block_addr, 
                                                                                  lba_access_dict, 
                                                                                  trace_stat_dict,
                                                                                  sample_lba_dict)
            sample_lba_dict[region_block_addr] = 1 
            trace_stat_dict = new_trace_stat_dict

        print(best_region_entry)
        print(type(best_region_entry))

        create_sample_trace(processor._full_df, sample_lba_dict, self.test_post_process_sample_file_path)
        sample_df = processor.load_block_trace(str(self.test_post_process_sample_file_path))
        file_trace_stat_dict = processor.get_overall_stat_from_df(sample_df)

        print(file_trace_stat_dict)
        print(processor._full_stat_dict)


    def test_get_per_block_access_stat_dict(self):
        trace_path = self.test_data_dir.joinpath("sample_trace1.csv")
        trace_df = PostProcess.load_block_trace(str(trace_path))
        sample_block_dict = {0: 1, 1: 1}
        per_block_access_dict = PostProcess.get_per_block_access_stat_dict(trace_df, sample_block_dict)
        print(dumps(per_block_access_dict, indent=2))

        for block_key in sample_block_dict:
            assert block_key not in per_block_access_dict, \
                "Block key {} is sampled and should not contain access stats for it in the access dict.".format(block_key)

        # LBA 0, 4 and 9 have solo accesses, but 0 is already sampled 
        for lba_key in [4, 9]:
            assert per_block_access_dict[lba_key]["r_solo_count"] > 0 or per_block_access_dict[lba_key]["w_solo_count"] > 0, \
                "LBA key {} should have solo accesses.".format(lba_key)
        
        # LBA 7 should have left and right access
        assert per_block_access_dict[7]["w_right_count"] > 0 and per_block_access_dict[7]["w_left_count"] > 0, \
            "LBA key 7 sould have an accesses where it is the left most and right most block."

        assert per_block_access_dict[8]["w_right_count"] > 0, \
            "LBA key 8 sould have an access where it is the right most block."
    

    def test_priority_metrics(self):
        processor = PostProcess(str(self.test_block_trace_path.expanduser()), str(self.test_sample_block_dir_path.expanduser()))
        sample_lba_dict = deepcopy(processor._sample_lba_dict)
        trace_stat_dict = deepcopy(processor._sample_stat_dict)
        lba_added = 0 
        max_lba_added = 2 
        for lba_key in processor.per_unsampled_block_stat:
            lba_access_dict = processor.per_unsampled_block_stat[lba_key]
            metric_dict, new_trace_stat_dict = processor.compute_priority_metrics(lba_key, 
                                                                                  lba_access_dict, 
                                                                                  trace_stat_dict,
                                                                                  sample_lba_dict)
            sample_lba_dict[lba_key] = 1 
            create_sample_trace(processor._full_df, sample_lba_dict, self.test_post_process_sample_file_path)
            sample_df = processor.load_block_trace(str(self.test_post_process_sample_file_path))
            file_trace_stat_dict = processor.get_overall_stat_from_df(sample_df)
            metric_dict["it"] = lba_added
            metric_dict["key"] = lba_key
            print(metric_dict)
            for stat_key in new_trace_stat_dict:
                stat_value = new_trace_stat_dict[stat_key]
                file_value = file_trace_stat_dict[stat_key]
                assert file_value == stat_value, \
                    "File and compute stat {}, {} do not match for stat.".format(file_value, stat_value, stat_key)
            lba_added += 1 
            trace_stat_dict = new_trace_stat_dict

            if lba_added == max_lba_added:
                break 


if __name__ == '__main__':
    main()