"""Given a block trace and a sample, this class iteratively adds blocks to the sample
trace to reduce mean and/or std deviation of mean error values. 

Usage:
    post_processor = PostProcess(block_trace_path, sample_trace_path)

    # post-process a sample and generate a new sample block trace 
    post_processor.process(max_hit_rate, err_file_path, metadata_file_path, sample_file_path, priority_metric="req")
"""

from unittest.mock import NonCallableMagicMock
from numpy import mean, std, array, ndarray, zeros
from pathlib import Path 
from typing import Tuple
from copy import deepcopy
from json import dumps, dump 
from queue import PriorityQueue
from time import perf_counter_ns
from pandas import read_csv, DataFrame

from cydonia.sample.Sample import create_sample_trace


class PostProcess:
    def __init__(
            self, 
            block_trace_path: str, 
            sample_trace_path: str 
    ) -> None:
        """Initiate post processing by loading the full and sample block trace.  

        Args:
            block_trace_path: Path to full block trace. 
            sample_trace_path: Path to sample block trace. 
        
        Attributes:
            _time_stats: Dictionary of stats related to time taken by different steps of post processing. 
            _full_df: DataFrame containing the full block trace. 
            _full_stat_dict: Dictionary of full trace statistics. 
            _full_lba_set: Set of LBAs accessed in the full block trace. 
            _full_lba_count: Number of unique LBAs in the full block trace. 
            _sample_df: DataFrame containing the sample block trace. 
            _sample_stat_dict: Dictionary of sample trace statistics. 
            _sample_lba_set: Set of LBAs accessed in the sample block trace. 
            _sample_lba_count: Number of unique LBAs in the sample block trace. 
            _total_lba_count: Number of unique LBAs in the full block trace. 
            _lba_stat_dict: Dictionary of features of each LBA that was not sampled. 
        """
        self._time_stats = {}

        # load the block trace 
        start_time = perf_counter_ns()
        self._full_df = self.load_block_trace(block_trace_path)
        end_time = perf_counter_ns()
        self._time_stats["load_trace"] = self.ns_to_min(end_time-start_time)

        # compute stats from block trace 
        start_time = perf_counter_ns()
        self._full_stat_dict = self.get_overall_stat_from_df(self._full_df)
        self._full_lba_set = self.get_unique_lba_set(self._full_df)
        self._full_lba_count = len(self._full_lba_set)
        end_time = perf_counter_ns()
        self._time_stats["compute_trace_stats"] = self.ns_to_min(end_time-start_time)

        # load the sample block trace 
        start_time = perf_counter_ns()
        self._sample_df = self.load_block_trace(sample_trace_path)
        end_time = perf_counter_ns()
        self._time_stats["load_sample_trace"] = self.ns_to_min(end_time-start_time)

        # compute stats from sample block trace 
        start_time = perf_counter_ns()
        self._sample_stat_dict = self.get_overall_stat_from_df(self._sample_df)
        self._sample_lba_set = self.get_unique_lba_set(self._sample_df)
        self._sample_lba_count = len(self._sample_lba_set)
        self._sample_lba_dict = dict.fromkeys(self.get_unique_lba_set(self._sample_df), 1)
        self._sample_percent_error_dict = self.get_percent_error_dict(self._full_stat_dict, self._sample_stat_dict)
        self._cur_eff_sample_rate = 100.0 * len(self._sample_lba_set)/len(self._full_lba_set)
        end_time = perf_counter_ns()
        self._time_stats["compute_sample_trace_stats"] = self.ns_to_min(end_time-start_time)

        # load stats for each unsampled block 
        start_time = perf_counter_ns()
        self._per_unsampled_block_stat = self.get_per_block_access_stat_dict(self._full_df, self._sample_lba_dict)
        end_time = perf_counter_ns()
        self._time_stats["load_lba_stats"] = self.ns_to_min(end_time-start_time)

        print("{}".format(dumps(self._time_stats)))
    

    @property
    def per_unsampled_block_stat(self):
        return self._per_unsampled_block_stat
            

    def compute_priority_metrics(
            self,
            lba_key: int,
            lba_stat_dict: dict,
            sample_trace_stat_dict: dict,
            sample_lba_dict: dict 
    ) -> Tuple:
        """Compute priority metrics of an LBA with the given stats. 

        Args:
            lba_stat_dict: Dictionary with statistics related to an LBA. 
            lba_key: The LBA key to which the dictionary of stats belongs. 
            sample_trace_stat_dict: Dictionary with statistics of the sample trace. 

        Returns:
            metric_dict: Dictionary with different priority metrics computed. 
        """      
        # compute how many new, join or add events will happen if we sample this block 
        left_sampled = True if lba_key-1 in sample_lba_dict else False 
        right_sampled = True if lba_key+1 in sample_lba_dict else False 
        read_add_new_join_stat_dict = self.get_add_new_join_stat_dict(lba_stat_dict, left_sampled, right_sampled, 'r')
        write_add_new_join_stat_dict = self.get_add_new_join_stat_dict(lba_stat_dict, left_sampled, right_sampled, 'w')
        add_new_join_stat_dict = {**read_add_new_join_stat_dict, **write_add_new_join_stat_dict}

        # compute the feature of the trace with the new block included and the percent error 
        new_trace_stat_dict = self.get_new_trace_stat(sample_trace_stat_dict, add_new_join_stat_dict)
        new_percent_error_dict = self.get_percent_error_dict(self._full_stat_dict, new_trace_stat_dict)

        # collect error data 
        metric_dict = {}
        percent_error_diff_arr, err_arr = [], []
        for error_key in new_percent_error_dict:
            metric_dict[error_key] = new_percent_error_dict[error_key]
            metric_dict["delta_{}".format(error_key)] = abs(self._sample_percent_error_dict[error_key]) - abs(new_percent_error_dict[error_key])
            percent_error_diff_arr.append(metric_dict["delta_{}".format(error_key)])
            err_arr.append(abs(new_percent_error_dict[error_key]))

        metric_dict["delta_rev_err"] = float(mean(percent_error_diff_arr))
        metric_dict["delta_err"] = -1 * metric_dict["delta_rev_err"]
        metric_dict["delta_std_dev"] = std(percent_error_diff_arr)
        metric_dict["std_dev"] = std(err_arr)
        metric_dict["err"] = float(mean(err_arr))
        metric_dict["newreq"] = add_new_join_stat_dict["r_new_count"] + add_new_join_stat_dict["w_new_count"]
        return metric_dict, new_trace_stat_dict
    

    def get_priority_queue(
            self,
            priority_metric: str,
            sample_lba_dict: dict 
    ) -> PriorityQueue:
        """Get a priority queue with the specified priority metric. 

        Args:
            priority_metric: The metric used to assign priority when picking blocks. Types of priority metric:
            sample_lba_dict: Dictionary with sampled blocks as keys. 
        
        Return:
            priority_queue: PriorityQueue loaded with priority metric and corresponding block statistics. 
        """
        queue = PriorityQueue()
        for lba_key in self._per_unsampled_block_stat:
            priority_metric_dict, _ = self.compute_priority_metrics(int(lba_key), 
                                                                    self._per_unsampled_block_stat[lba_key], 
                                                                    self._sample_stat_dict, 
                                                                    sample_lba_dict)
            queue.put((priority_metric_dict[priority_metric], lba_key))
        return queue 


    def postprocess(
        self,
        max_sample_rate: int,
        error_file_path: Path, 
        metadata_file_path: Path,
        sample_file_path: Path,
        priority_metric: str = "delta_err"
    ) -> None:
        """Post process a sample using two queues. 

        Args:
            max_sample_rate: Maximum percentage of LBAs that can be included in the sample. 
            error_file_path: Output path of the file where error values are written after each iteration. 
            metadata_file_path: Path where JSON file of sample metadata is created. 
            sample_file_path: Path where the new sample will be generated. 
            priority_metric: Metric used to prioritize which LBA is added first. 
        """
        cur_sample_rate = 100.0 * len(self._sample_lba_set)/len(self._full_lba_set)
        if cur_sample_rate >= max_sample_rate:
            print("Cur sample rate {} already higher than max sample rate {}.".format(cur_sample_rate, max_sample_rate))
            return 

        # create a priority of queue to prioritize certain LBAs for addition 
        start_time = perf_counter_ns()
        old_queue = self.get_priority_queue(priority_metric, self._sample_lba_dict)
        end_time = perf_counter_ns()
        self._time_stats["load_priority_queue"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))

        low_err_metric = 0.0 
        low_err_metric_dict = {}
        old_lba_added, new_lba_added = 0, 0 
        lba_added = 0 
        updated_lba_dict = {}
        new_queue_item, old_queue_item = None, None 
        sample_lba_dict = deepcopy(self._sample_lba_dict)
        cur_trace_stat_dict = deepcopy(self._sample_stat_dict)
        new_queue = PriorityQueue()
        metadata_arr = []
        print("Priority queue size: {}".format(old_queue.qsize()))
        while new_queue.qsize() or old_queue.qsize() or new_queue_item or old_queue_item:
            if 100.0*(self._sample_lba_count+lba_added)/self._full_lba_count > max_sample_rate:
                break 

            # get the best item from both the queues 
            if new_queue.qsize() and new_queue_item is None:
                # get an item from the new queue 
                # check if this item has already been sampled 
                # Sometimes when a single block data is updated multiple times
                # it can have multiple entries in the new queue and hence, 
                # a block from the new queue might already be sampled. 
                new_queue_item = new_queue.get()
                while new_queue_item[0] in sample_lba_dict:
                    if new_queue.qsize():
                        new_queue_item = new_queue.get()
                    else:
                        new_queue_item = None
                        break 

            if old_queue.qsize() and old_queue_item is None:
                old_queue_item = old_queue.get()
                # if an item has an updated metric value then skip that item,
                # since we will get that item from the new queue 
                while old_queue_item[0] in updated_lba_dict:
                    if old_queue.qsize():
                        old_queue_item = old_queue.get()
                    else:
                        old_queue_item = None 
                        break 

            # select the lba key to be added next to the trace 
            if old_queue_item is not None and new_queue_item is None:
                # the new queue is empty but the old queue is not so simple take from the old queue 
                metric_value, lba_key = old_queue_item
                old_queue_item = None 
            elif old_queue_item is None and new_queue_item is not None:
                metric_value, lba_key = new_queue_item
                new_queue_item = None 
            elif old_queue_item is not None and new_queue_item is not None:
                # we have items from both old and new queue, compare the metric value and choose the lower one 
                # prioritize new queue or old queue? For now the new queue.
                old_metric_value, old_lba_key = old_queue_item
                new_metric_value, new_lba_key = new_queue_item
                if new_metric_value <= old_metric_value:
                    metric_value, lba_key = new_metric_value, new_lba_key
                    new_queue_item = None
                    new_lba_added += 1
                else:
                    metric_value, lba_key = old_metric_value, old_lba_key
                    old_queue_item = None 
                    old_lba_added += 1
            else:
                continue 

            # add a block to the trace 
            lba_stat_dict = self._per_unsampled_block_stat[lba_key]
            priority_metric_dict, cur_trace_stat_dict = self.compute_priority_metrics(lba_key, lba_stat_dict, cur_trace_stat_dict, sample_lba_dict)
            sample_lba_dict[lba_key] = 1
            lba_added += 1

            if lba_key-1 not in sample_lba_dict and lba_key-1 in self._per_unsampled_block_stat:
                # if left block is not yet sampled, but accessed we update stats if this block is ever accessed alongside its left block 
                if lba_stat_dict['r_right_count'] or lba_stat_dict['w_right_count']:
                    metric_dict, _ = self.compute_priority_metrics(lba_key-1, lba_stat_dict, cur_trace_stat_dict, sample_lba_dict)
                    new_queue.put((metric_dict[priority_metric], lba_key-1))
                    updated_lba_dict[lba_key-1] = priority_metric

            if lba_key+1 not in sample_lba_dict and lba_key+1 in self._per_unsampled_block_stat:
                # if right block is not yet sampled, but accessed we update stats if this block is ever accessed alongside its right block 
                if lba_stat_dict['r_left_count'] or lba_stat_dict['w_left_count']:
                    metric_dict, _ = self.compute_priority_metrics(lba_key+1, lba_stat_dict, cur_trace_stat_dict, sample_lba_dict)
                    new_queue.put((metric_dict[priority_metric], lba_key+1))
                    updated_lba_dict[lba_key+1] = priority_metric
            
            priority_metric_dict["it"] = lba_added
            priority_metric_dict["key"] = lba_key
            priority_metric_dict["rate"] = 100.0 * (self._sample_lba_count+lba_added)/len(self._full_lba_set)
            priority_metric_dict["metric"] = priority_metric
            metadata_arr.append(priority_metric_dict)
            print(dumps(priority_metric_dict, indent=2))

        end_time = perf_counter_ns()
        self._time_stats["add_lba"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))

        # save the trace once post processing is done 
        start_time = perf_counter_ns()
        df = DataFrame(metadata_arr)
        df.to_csv(error_file_path, index=False)
        create_sample_trace(self._full_df, sample_lba_dict, sample_file_path)
        new_sample_df = self.load_block_trace(str(sample_file_path))
        metadata_dict = self.get_sample_error_dict(self._full_df, new_sample_df)
        metadata_dict["time"] = self._time_stats
        metadata_dict["start_error"] = self._sample_percent_error_dict
        metadata_dict["max_rate"] = max_sample_rate
        metadata_dict["rate"] = 100.0 * (self._sample_lba_count+lba_added)/len(self._full_lba_set)
        metadata_dict["old_lba_added"] = old_lba_added
        metadata_dict["new_lba_added"] = new_lba_added 
        print(metadata_dict)
        with metadata_file_path.open("w+") as metadata_file_handle:
            dump(metadata_dict, metadata_file_handle, indent=2)
        end_time = perf_counter_ns()
        self._time_stats["save_data"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))
            

    def process(
            self, 
            max_sample_rate: int,
            error_file_path: Path, 
            metadata_file_path: Path,
            sample_file_path: Path,
            priority_metric: str = "delta_err"
    ) -> None:
        """Post process the loaded sample and block trace and generate a new sample block trace. 

        Args:
            max_sample_rate: Maximum percentage of LBAs that can be included in the sample. 
            error_file_path: Output path of the file where error values are written after each iteration. 
            metadata_file_path: Path where JSON file of sample metadata is created. 
            sample_file_path: Path where the new sample will be generated. 
            priority_metric: Metric used to prioritize which LBA is added first. 
        """
        if self._cur_eff_sample_rate >= max_sample_rate:
            print("Cur sample rate {} already higher than max sample rate {}.".format(self._cur_eff_sample_rate, max_sample_rate))
            return 

        # create a priority of queue to prioritize certain LBAs for addition 
        start_time = perf_counter_ns()
        priority_queue = self.get_priority_queue(priority_metric, self._sample_lba_dict)
        end_time = perf_counter_ns()
        self._time_stats["load_priority_queue"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))

        # pop the priority queue and add LBAs to the sample trace until you reach a limit 
        start_time = perf_counter_ns()
        lba_added = 0 
        cur_sample_lba_count = self._sample_lba_count
        new_trace_stat_dict = deepcopy(self._sample_stat_dict)
        sample_lba_dict = deepcopy(self._sample_lba_dict)
        metadata_arr = []
        for _ in range(priority_queue.qsize()):
            if 100.0*(self._sample_lba_count+lba_added)/self._full_lba_count > max_sample_rate:
                break 

            priority_metric, lba_key = priority_queue.get()
            lba_stat_dict = self._per_unsampled_block_stat[lba_key]
            priority_metric_dict, temp_trace_stat_dict = self.compute_priority_metrics(lba_key, lba_stat_dict, new_trace_stat_dict, sample_lba_dict)
            if priority_metric_dict["delta_rev_err"] > 0:
                new_trace_stat_dict = temp_trace_stat_dict
                lba_added += 1

                priority_metric_dict["it"] = lba_added
                priority_metric_dict["key"] = lba_key
                priority_metric_dict["rate"] = 100.0 * (cur_sample_lba_count+lba_added)/len(self._full_lba_set)
                priority_metric_dict["metric"] = priority_metric
                metadata_arr.append(priority_metric_dict)
                sample_lba_dict[lba_key] = 1 
                print(dumps(priority_metric_dict, indent=2))

        end_time = perf_counter_ns()
        self._time_stats["add_lba"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))

        # save the trace once post processing is done 
        start_time = perf_counter_ns()
        df = DataFrame(metadata_arr)
        df.to_csv(error_file_path, index=False)
        create_sample_trace(self._full_df, sample_lba_dict, sample_file_path)
        new_sample_df = self.load_block_trace(str(sample_file_path))
        metadata_dict = self.get_sample_error_dict(self._full_df, new_sample_df)
        metadata_dict["time"] = self._time_stats
        metadata_dict["start_error"] = self._sample_percent_error_dict
        metadata_dict["max_rate"] = max_sample_rate
        print(metadata_dict)
        with metadata_file_path.open("w+") as metadata_file_handle:
            dump(metadata_dict, metadata_file_handle, indent=2)
        end_time = perf_counter_ns()
        self._time_stats["save_data"] = self.ns_to_min(end_time-start_time)
        print("{}".format(self._time_stats))


    @staticmethod 
    def get_block_addr_arr(
            block_addr: int,
            num_lower_order_bits_ignored: int 
    ) -> ndarray:
        """Get the array of blocks in a region, whose size is determined by the number of lower
        order bits ignored, which consists of the given block address. 

        Args:
            block_addr: The address of the block to which the region belongs to. 
            num_lower_order_bits_ignored: Number of lower order bits ignored. 
        
        Returns:
            block_addr_arr: Array of block addresses in the region containing the given block address. 
        """
        region_index = block_addr >> num_lower_order_bits_ignored
        num_block_in_region = 2**num_lower_order_bits_ignored
        block_addr_arr = zeros(num_block_in_region, dtype=int)
        for block_index in range(num_block_in_region):
            block_addr_arr[block_index] = (region_index << num_lower_order_bits_ignored) + block_index
        return block_addr_arr


    @staticmethod 
    def get_per_region_error_df(
            per_unsampled_block_stat_dict: dict,
            num_lower_order_bits_ignored: int,  
            block_sample_dict: dict,
            sample_workload_stat_dict: dict,
            full_workload_stat_dict: dict,
            std_flag: bool = True
    ) -> DataFrame:
        """Compute a DataFrame where each row gives details of a region that reduces mean error
        if added to the sample. 

        Args:
            per_unsampled_block_stat_dict: Dictionary of statistics related to each unsampled block. 
            num_lower_order_bits_ignored: Number of lower order bits ignored. 
            block_sample_dict: Dictionary with sampled block address as keys. 
            sample_workload_stat_dict: Dictionary with workload stat of the sample trace. 
            full_workload_stat_dict: Dictionary with workload stat of the full trace. 
            std_flag: Flag indicating whether to consider standard deviation along with mean error when adding blocks. 

        Returns:
            region_df: DataFrame of error values when adding a region to the sample trace. 
        """
        error_arr = []
        region_computed_dict = {}

        # get the current mean and std dev of error values in the sample 
        sample_err_dict = PostProcess.get_percent_error_dict(full_workload_stat_dict, sample_workload_stat_dict)
        sample_err_arr = [sample_err_dict[err_key] for err_key in sample_err_dict]
        sample_err_mean, sample_err_std_dev = mean(sample_err_arr), std(sample_err_arr)

        # iterate through each unsampled block 
        for unsampled_block_addr in per_unsampled_block_stat_dict:
            region_index = unsampled_block_addr >> num_lower_order_bits_ignored
            # if the block is already sampled
            if unsampled_block_addr in block_sample_dict or region_index in region_computed_dict:
                continue 
            
            total_add_new_join_stat_dict = {}
            region_block_addr_arr = PostProcess.get_block_addr_arr(unsampled_block_addr, num_lower_order_bits_ignored)
            assert len(region_block_addr_arr), "Empty array of block addresses found."
            if len(region_block_addr_arr) == 1:
                region_block_addr = region_block_addr_arr[0]
                left_sampled = region_block_addr-1 in block_sample_dict
                right_sampled = region_block_addr+1 in block_sample_dict

                read_add_new_join_stat_dict = PostProcess.get_add_new_join_stat_dict(per_unsampled_block_stat_dict[region_block_addr],
                                                                                left_sampled, right_sampled, 'r')
                write_add_new_join_stat_dict = PostProcess.get_add_new_join_stat_dict(per_unsampled_block_stat_dict[region_block_addr],
                                                                                left_sampled, right_sampled, 'w')
                add_new_join_stat_dict = {**read_add_new_join_stat_dict, **write_add_new_join_stat_dict}
                total_add_new_join_stat_dict = deepcopy(add_new_join_stat_dict)
            else:
                for region_block_addr in region_block_addr_arr:
                    if region_block_addr not in per_unsampled_block_stat_dict:
                        continue 

                    left_block_addr, right_block_addr = region_block_addr - 1, region_block_addr + 1 

                    if region_block_addr == region_block_addr_arr[0]:
                        left_sampled = left_block_addr in block_sample_dict
                        right_sampled = right_block_addr in per_unsampled_block_stat_dict or right_block_addr in block_sample_dict
                    elif region_block_addr == region_block_addr_arr[-1]:
                        left_sampled = left_block_addr in per_unsampled_block_stat_dict or left_block_addr in block_sample_dict
                        right_sampled = right_block_addr in block_sample_dict
                    else:
                        left_sampled = left_block_addr in per_unsampled_block_stat_dict or left_block_addr in block_sample_dict
                        right_sampled = right_block_addr in per_unsampled_block_stat_dict or right_block_addr in block_sample_dict
                    
                    read_add_new_join_stat_dict = PostProcess.get_add_new_join_stat_dict(per_unsampled_block_stat_dict[region_block_addr],
                                                                                    left_sampled, right_sampled, 'r')
                    write_add_new_join_stat_dict = PostProcess.get_add_new_join_stat_dict(per_unsampled_block_stat_dict[region_block_addr],
                                                                                    left_sampled, right_sampled, 'w')
                    add_new_join_stat_dict = {**read_add_new_join_stat_dict, **write_add_new_join_stat_dict}

                    if not total_add_new_join_stat_dict:
                        total_add_new_join_stat_dict = deepcopy(add_new_join_stat_dict)
                    else:
                        for key in add_new_join_stat_dict:
                            total_add_new_join_stat_dict[key] += add_new_join_stat_dict[key]
            
            new_workload_stat_dict = PostProcess.get_new_trace_stat(sample_workload_stat_dict, total_add_new_join_stat_dict)
            new_err_dict = PostProcess.get_percent_error_dict(full_workload_stat_dict, new_workload_stat_dict)
            new_err_arr = [new_err_dict[err_key] for err_key in new_err_dict]
            new_err_mean, new_err_std_dev = mean(new_err_arr), std(new_err_arr)
            new_err_dict['region'] = region_index
            new_err_dict['block'] = unsampled_block_addr
            new_err_dict['mean'] = new_err_mean 
            new_err_dict['std_dev'] = new_err_std_dev

            percent_mean_err_diff = 100.0*(sample_err_mean - new_err_mean)/sample_err_mean
            percent_std_dev_err_diff = 100.0*(sample_err_mean - new_err_mean)/sample_err_mean
            new_err_dict["percent_mean_err_diff"] = percent_mean_err_diff
            new_err_dict["percent_std_dev_err_diff"] = percent_std_dev_err_diff
            for new_req_type in total_add_new_join_stat_dict:
                new_err_dict[new_req_type] = total_add_new_join_stat_dict[new_req_type]
            for new_workload_stat in new_workload_stat_dict:
                new_err_dict['new_{}'.format(new_workload_stat)] = new_workload_stat_dict[new_workload_stat]
            region_computed_dict[region_index] = True 

            if std_flag:
                if new_err_mean < sample_err_mean and new_err_std_dev <= sample_err_std_dev:
                    error_arr.append(new_err_dict)
            else:
                if new_err_mean < sample_err_mean:
                    error_arr.append(new_err_dict)

        return DataFrame(error_arr)


    @staticmethod
    def get_multi_per_region_dict(
            per_unsample_block_stat_dict: dict,
            region_size_block_array: list,
            sample_block_dict: dict 
    ) -> dict:
        """Get the add,new and join stats for a region. 

        Args:
            per_unsampled_block_stat_dict: Dictionary of solo, right, left and mid access to unsampled blocks. 
            region_size_num_block_array: Array of region sizes in number of blocks. 
            sample_block_dict: Dictionary with sampled block address as keys. 
        
        Returns:
            per_region_stat_dict: Dictionary of solo, right, left and mid access with region size in number of blocks as the key. 
        """
        per_region_stat_dict = {}
        # iterate through each unsampled block 
        # update 12,8,4,2,0 dict 

        # compute the 12, update the 12 dict with the necessary assumptions 
        return per_region_stat_dict 


    @staticmethod
    def ns_to_min(ns_val: int) -> float:
        return float(ns_val/(1e9*60))


    @staticmethod
    def get_unique_lba_set(
        df: DataFrame,
        lba_size_byte: int = 512 
    ) -> set:
        unique_lba_set = set()
        for _, row in df.iterrows():
            start_lba, size_byte, op = row["lba"], row["size"], row["op"]
            assert size_byte>0 and size_byte%lba_size_byte==0,\
                "Size {} has to be multiple of 512.".format(size_byte)
            size_block = int(size_byte/lba_size_byte)
            for cur_lba in range(start_lba, start_lba+size_block):
                unique_lba_set.add(cur_lba)
        return unique_lba_set


    @staticmethod
    def get_sample_error_dict(
        full_df: DataFrame, 
        sample_df: DataFrame
    ) -> dict:
        """Get percent error from full and sample DataFrame. 
        
        Args:
            full_df: DataFrame with the full block trace. 
            sample_df: DataFrame with the sample block trace. 
        
        Returns:
            percent_error_dict: Dictionary of percent error of select features. 
        """
        full_trace_stat = PostProcess.get_overall_stat_from_df(full_df)
        sample_trace_stat = PostProcess.get_overall_stat_from_df(sample_df)
        return PostProcess.get_percent_error_dict(full_trace_stat, sample_trace_stat)


    @staticmethod
    def get_percent_error_dict(
        full_stat_dict: dict, 
        sample_stat_dict: dict
    ) -> dict:
        """Get dictionary of percent error from dictionary of full and sample trace stats. 
        
        Args:
            self._full_stat_dict: Dictionary of full trace stats. 
            self._sample_stat_dict: Dictionary of sample trace stats. 
        
        Returns:
            percent_error_dict: Dictionary of percent error of select features. 
        """
        percent_error_dict = {}

        full_mean_read_size = full_stat_dict["total_read_size"]/full_stat_dict["read_count"]
        sample_mean_read_size = sample_stat_dict["total_read_size"]/sample_stat_dict["read_count"]
        percent_error_dict["mean_read_size"] = 100.0*(full_mean_read_size - sample_mean_read_size)/full_mean_read_size

        full_mean_write_size = full_stat_dict["total_write_size"]/full_stat_dict["write_count"]
        sample_mean_write_size = sample_stat_dict["total_write_size"]/sample_stat_dict["write_count"]
        percent_error_dict["mean_write_size"] = 100.0*(full_mean_write_size - sample_mean_write_size)/full_mean_write_size

        full_mean_read_iat = full_stat_dict["total_read_iat"]/full_stat_dict["read_count"]
        sample_mean_read_iat = sample_stat_dict["total_read_iat"]/sample_stat_dict["read_count"]
        percent_error_dict["mean_read_iat"] = 100.0*(full_mean_read_iat - sample_mean_read_iat)/full_mean_read_iat

        full_mean_write_iat = full_stat_dict["total_write_iat"]/full_stat_dict["write_count"]
        sample_mean_write_iat = sample_stat_dict["total_write_iat"]/sample_stat_dict["write_count"]
        percent_error_dict["mean_write_iat"] = 100.0*(full_mean_write_iat - sample_mean_write_iat)/full_mean_write_iat

        full_write_ratio = full_stat_dict["write_count"]/(full_stat_dict["read_count"] + full_stat_dict["write_count"])
        sample_write_ratio = sample_stat_dict["write_count"]/(sample_stat_dict["read_count"] + sample_stat_dict["write_count"])
        percent_error_dict["write_ratio"] = 100.0 * (full_write_ratio - sample_write_ratio)/full_write_ratio

        return percent_error_dict


    @staticmethod
    def get_overall_stat_from_df(df: DataFrame) -> dict:
        """Get the statistics from a DataFram with the block trace.
        
        Args:
            df: DataFrame with the block trace. 
        
        Returns:
            stat_dict: Dictionary of overall stat from block trace df. 
        """
        stat_dict = {}
        stat_dict["read_count"] = len(df[df['op']=='r'])
        stat_dict["write_count"] = len(df[df['op']=='w'])
        stat_dict["total_read_size"] = df[df['op']=='r']['size'].sum()
        stat_dict["total_write_size"] =  df[df['op']=='w']['size'].sum()
        stat_dict["total_read_iat"] = df[df['op']=='r']['iat'].sum()
        stat_dict["total_write_iat"] = df[df['op']=='w']['iat'].sum()
        stat_dict["write_ratio"] = stat_dict["write_count"]/(stat_dict["read_count"] + stat_dict["write_count"])
        return stat_dict 
        

    @staticmethod
    def load_block_trace(
        trace_path: str
    ) -> DataFrame:
        """Load a block trace file into a pandas DataFrame.  
        
        Args:
            trace_path: Path to block trace. 
        
        Returns:
            df: Block trace with additional features as a DataFrame. 
        """
        df = read_csv(trace_path, names=["ts", "lba", "op", "size"])
        df["iat"] = df["ts"].diff()
        return df 


    @staticmethod
    def init_lba_access_dict():
        """Get the template for LBA access dict. 

        Returns:
            access_dict: Dictionary with all keys of LBA stats initiated to 0. 
        """
        return {
            "r_solo_count": 0,
            "w_solo_count": 0,
            "r_solo_iat_sum": 0, 
            "w_solo_iat_sum": 0,

            "r_right_count": 0,
            "w_right_count": 0,
            "r_right_iat_sum": 0, 
            "w_right_iat_sum": 0, 

            "r_left_count": 0,
            "w_left_count": 0,
            "r_left_iat_sum": 0, 
            "w_left_iat_sum": 0, 

            "r_mid_count": 0,
            "w_mid_count": 0,
            "r_mid_iat_sum": 0, 
            "w_mid_iat_sum": 0
        }
    

    @staticmethod
    def get_per_block_access_stat_dict(
        trace_df: DataFrame,
        sample_block_dict: dict,
        block_size_byte: int = 512 
    ) -> dict:
        """Get a dictionary with the unsampled block addresses as keys and a dictionary of
        access statistics as value.
        
        Args:
            trace_df: DataFrame of block trace. 
            sample_block_dict: Dictionary with sampled block addresses as keys for lookup. 
            block_size_byte: Size of each block in bytes. (Default: 512)
        
        Returns:
            per_block_access_stat_dict: Dictionary with unsampled block addresses as keys and dictionary
                                            of access statistics as value. 
        """
        per_block_stat_dict = {}
        for _, row in trace_df.iterrows():
            block_address, size_byte, op = int(row["lba"]), int(row["size"]), row["op"]
            size_block = size_byte//block_size_byte

            try:
                cur_iat = int(row["iat"])
            except ValueError:
                cur_iat = 0
            
            start_lba = block_address
            end_lba = block_address + size_block
            for cur_lba in range(start_lba, end_lba):
                if cur_lba in sample_block_dict:
                    continue 

                if cur_lba not in per_block_stat_dict:
                    per_block_stat_dict[cur_lba] = dict(PostProcess.init_lba_access_dict())

                if size_block == 1:
                    per_block_stat_dict[cur_lba]["{}_solo_count".format(op)] += 1 
                    per_block_stat_dict[cur_lba]["{}_solo_iat_sum".format(op)] += cur_iat
                else:
                    if cur_lba == start_lba:
                        per_block_stat_dict[cur_lba]["{}_left_count".format(op)] += 1 
                        per_block_stat_dict[cur_lba]["{}_left_iat_sum".format(op)] += cur_iat
                    elif cur_lba == start_lba+size_block-1:
                        per_block_stat_dict[cur_lba]["{}_right_count".format(op)] += 1 
                        per_block_stat_dict[cur_lba]["{}_right_iat_sum".format(op)] += cur_iat
                    else:
                        per_block_stat_dict[cur_lba]["{}_mid_count".format(op)] += 1 
                        per_block_stat_dict[cur_lba]["{}_mid_iat_sum".format(op)] += cur_iat

        return per_block_stat_dict


    @staticmethod
    def get_add_new_join_stat_dict(
            lba_stat_dict: dict,
            left_sampled: bool,
            right_sampled: bool,
            op: str 
    ) -> dict:
        """Get the add, new and join statistics based on LBA access stats and sample 
        status of LBA-1 and LBA+1. 

        Args:
            lba_stat_dict: The access statistics of an LBA. 
            left_sampled: Boolean indicating whether LBA-1 is sampled. 
            right_sampled: Boolean indicating whether LBA+1 is sampled. 
            op: The operation type of access ('r' or 'w').
        
        Returns:
            add_new_join_stat_dict: Dictionary with add, new and join count and IAT sum. 
        """
        add_count, new_count, join_count = 0, lba_stat_dict["{}_solo_count".format(op)], 0
        add_iat_sum, new_iat_sum, join_iat_sum = 0, lba_stat_dict["{}_solo_iat_sum".format(op)], 0

        if left_sampled and right_sampled:
            # since both left and right blocks are sampled, any block request where this LBA 
            # was in the moddle was split in the sample, but will be joined if we sampled this LBA 
            join_count += lba_stat_dict["{}_mid_count".format(op)]
            join_iat_sum += lba_stat_dict["{}_mid_iat_sum".format(op)]

            # the left and right leaning lba access will just add to current block request 
            add_count += (lba_stat_dict["{}_left_count".format(op)]+lba_stat_dict["{}_right_count".format(op)])
            add_iat_sum += (lba_stat_dict["{}_left_iat_sum".format(op)]+lba_stat_dict["{}_right_iat_sum".format(op)])

            # no new block request will be included in the sample if we sample this LBA apart from block request
            # that access just this LBA 
        elif left_sampled and not right_sampled:
            # since right block is not sampled, there will be no block request splits that will be fixed

            # since left is sampled any right leaning access will just add to block request 
            add_count += (lba_stat_dict["{}_right_count".format(op)] + lba_stat_dict["{}_mid_count".format(op)])
            add_iat_sum += (lba_stat_dict["{}_right_iat_sum".format(op)] + lba_stat_dict["{}_mid_iat_sum".format(op)])

            # since right block is not sampled, any left leaning access will create a new block request and add splitting 
            new_count += lba_stat_dict["{}_left_count".format(op)]
            new_iat_sum += lba_stat_dict["{}_left_iat_sum".format(op)]
        elif not left_sampled and right_sampled:
            # since left block is not sampled, there will be no block request splits that will be fixed

            # since right block is sampled, any left leaning access will just add to current block request 
            add_count += (lba_stat_dict["{}_left_count".format(op)] + lba_stat_dict["{}_mid_count".format(op)])
            add_iat_sum += (lba_stat_dict["{}_left_iat_sum".format(op)] + lba_stat_dict["{}_mid_iat_sum".format(op)])

            # since left is not sampled, any right leaning access will create a new block request and add splitting 
            new_count += lba_stat_dict["{}_right_count".format(op)]
            new_iat_sum += lba_stat_dict["{}_right_iat_sum".format(op)]
        else:
            # since left and right block is not sampled, there will be no block request splits that will be fixed

            # since left and right block is not sampled, any left or right learning access will create a new request not add to a block requst 
            new_count += (lba_stat_dict["{}_left_count".format(op)]+lba_stat_dict["{}_right_count".format(op)] + lba_stat_dict["{}_mid_count".format(op)])
            new_iat_sum += (lba_stat_dict["{}_left_iat_sum".format(op)]+lba_stat_dict["{}_right_iat_sum".format(op)] + lba_stat_dict["{}_mid_iat_sum".format(op)])

        return {
            "{}_add_count".format(op): add_count,
            "{}_add_iat_sum".format(op): add_iat_sum,
            "{}_new_count".format(op): new_count,
            "{}_new_iat_sum".format(op): new_iat_sum,
            "{}_join_count".format(op): join_count, 
            "{}_join_iat_sum".format(op): join_iat_sum 
        }


    @staticmethod
    def get_new_trace_stat(
            trace_stat_dict: dict, 
            add_new_join_stat_dict: dict,
            lba_size_byte: int = 512
    ) -> dict:
        """Get new trace statistics if an LBA with the specified add, new and join block request stats is added to
        the trace. 

        Args:
            trace_stat_dict: Dictionary of trace statistics before adding LBA. 
            add_new_join_stat_dict: Dictionary with information about how the trace stat will be influenced 
            lba_size_byte: Size of LBA in bytes. 
        
        Returns:
            new_trace_stat_dict: Dictionary of updated trace statistics after LBA is added. 
        """
        new_trace_stat_dict = deepcopy(trace_stat_dict)
        for stat_key in add_new_join_stat_dict:
            if 'r' == stat_key[0] and "count" in stat_key:
                new_trace_stat_dict["total_read_size"] += (lba_size_byte * add_new_join_stat_dict[stat_key])
            elif 'w' == stat_key[0] and "count" in stat_key:
                new_trace_stat_dict["total_write_size"] += (lba_size_byte * add_new_join_stat_dict[stat_key])
        new_trace_stat_dict["read_count"] += (add_new_join_stat_dict["r_new_count"] - add_new_join_stat_dict["r_join_count"])
        new_trace_stat_dict["write_count"] += (add_new_join_stat_dict["w_new_count"] - add_new_join_stat_dict["w_join_count"])
        new_trace_stat_dict["total_read_iat"] += (add_new_join_stat_dict["r_new_iat_sum"] - add_new_join_stat_dict["r_join_iat_sum"])
        new_trace_stat_dict["total_write_iat"] += (add_new_join_stat_dict["w_new_iat_sum"] - add_new_join_stat_dict["w_join_iat_sum"])
        new_trace_stat_dict["write_ratio"] = new_trace_stat_dict["write_count"]/(new_trace_stat_dict["read_count"] + new_trace_stat_dict["write_count"])
        return new_trace_stat_dict
    

    @staticmethod 
    def update_trace(
            trace_stat_dict: dict,
            add_new_join_stat_dict: dict,
            reduce_flag: bool,
            lba_size_byte: int = 512
    ) -> dict:
        """Update the trace statistics based on the count and total interarrival time
        of add, new and join 
        """
        return {}