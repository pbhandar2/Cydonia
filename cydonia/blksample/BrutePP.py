from pathlib import Path 
from copy import deepcopy
from time import perf_counter_ns
from pandas import DataFrame, concat, read_csv 

from cydonia.blksample.lib import get_feature_err_dict, get_blk_addr_arr
from cydonia.profiler.CacheTraceProfiler import load_cache_trace, get_workload_features_from_cache_trace


class BrutePP:
    def __init__(
            self,
            sample_cache_trace: Path,
            full_workload_feature_dict: dict,
            output_file_path: Path,
            algo_bits: int,
            break_flag: bool
    ) -> None:
        self._sample_cache_trace = sample_cache_trace
        self._sample_cache_trace_df = load_cache_trace(sample_cache_trace)
        self._sample_workload_feature_dict = get_workload_features_from_cache_trace(self._sample_cache_trace_df)
        self._full_workload_feature_dict = full_workload_feature_dict
        self._feature_err = get_feature_err_dict(self._full_workload_feature_dict, self._sample_workload_feature_dict)
        self._output_file_path = output_file_path
        self._algo_bits = algo_bits
        self._break_flag = break_flag


    def find_best_block_to_remove(self):
        """Find the best block to remove by generating a new cache trace to evaluate the effect of removing a block."""
        num_block_evaluated = 0 
        start_time = perf_counter_ns()
        min_err_dict, block_evaluation_tracker_dict = deepcopy(self._feature_err), {}

        for cache_block_addr in self._sample_cache_trace_df["key"].unique():

            region_addr = cache_block_addr >> self._algo_bits
            if region_addr in block_evaluation_tracker_dict:
                continue 
            
            num_block_evaluated += 1
            block_evaluation_tracker_dict[region_addr] = 1
            blk_arr = get_blk_addr_arr(region_addr, self._algo_bits)
            
            cache_trace_df = self._sample_cache_trace_df[~self._sample_cache_trace_df["key"].isin(blk_arr)]
            new_sample_cache_feature_dict = get_workload_features_from_cache_trace(cache_trace_df)
            new_err_dict = get_feature_err_dict(self._full_workload_feature_dict, new_sample_cache_feature_dict)

            if (new_err_dict["mean"] < min_err_dict["mean"]) or \
                    ((new_err_dict["mean"] == min_err_dict["mean"]) and (new_err_dict["std"] < min_err_dict["std"])):
                min_err_dict = new_err_dict
                min_err_dict["addr"] = cache_block_addr
                print("New min error found after {} evaluations. JSON -> {}".format(num_block_evaluated, min_err_dict))

                if self._break_flag:
                    break 

        if min_err_dict:
            min_err_dict["time_ns"] = perf_counter_ns() - start_time

        return min_err_dict
    

    def brute(self):
        """Keep removing the best block by generating a new trace to evaluate the effect of block removal."""
        min_err_dict = self.find_best_block_to_remove()
        while min_err_dict:
            df = DataFrame([min_err_dict])

            if self._output_file_path.exists():
                cur_df = read_csv(self._output_file_path)
                new_df = concat([df, cur_df], ignore_index=True)
                new_df.to_csv(self._output_file_path, index=False)
            else:
                df.to_csv(self._output_file_path, index=False)

            self._sample_cache_trace_df = self._sample_cache_trace_df[~self._sample_cache_trace_df["key"].isin([min_err_dict["addr"]])]
            self._feature_err = deepcopy(min_err_dict)
            min_err_dict = self.find_best_block_to_remove()