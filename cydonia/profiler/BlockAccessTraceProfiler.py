"""BlockAccessTraceProfiler """


import json 
import numpy as np 
import pandas as pd 
from cydonia.profiler.RDHistogram import RDHistogram

class BlockAccessTraceProfiler:
    def __init__(
        self, 
        block_access_trace_path: str 
    ) -> None:
        self.df = pd.read_csv(block_access_trace_path, names=["ts", "id", "op", "rd"])
        self.start_time_us = self.df.iloc[0]["ts"]
        self.end_time_us = self.df.iloc[-1]["ts"]
        self.trace_length_us = self.end_time_us - self.start_time_us 
        self.rd_hist_snapshot_window_size_sec = 3600 * 24
        self.id_set = set()
        self.rd_hist = RDHistogram()
        print("Block trace loaded!")
    

    def get_stat(self, index, wss_blocks):
        max_read_hit_rate = 0.0 
        output_stat = {}
        output_stat['index'] = index
        output_stat['wss_gb'] = (wss_blocks * 4096)/1e9
        for percent_working_set_size in range(10, 101, 10):
            cache_size = (percent_working_set_size/100)*wss_blocks
            max_read_hit_rate, read_hit_rate = self.rd_hist.get_read_hit_rate(int(cache_size))
            output_stat['hr_{}'.format(percent_working_set_size)] = read_hit_rate
            output_stat['size_{}'.format(percent_working_set_size)] = int(cache_size)
        output_stat["max_read_hit_rate"] = max_read_hit_rate
        return output_stat

    
    def profile(self, output_path = None):
        active_window_index = 0
        window_stat_arr = []
        for row_index, row in self.df.iterrows():
            self.rd_hist.update_rd(row["rd"], row["op"])
            self.id_set.add(row["id"])

            cur_time_us = row["ts"]
            time_elapsed_us = (cur_time_us - self.start_time_us)/(1e6*3600)
            cur_window_index = int((cur_time_us - self.start_time_us)//(1e6*self.rd_hist_snapshot_window_size_sec))
            if row_index % 10000000 == 0 and row_index > 0:
                print("{}/{} processed! {}%".format(row_index, len(self.df), 100*row_index/len(self.df)))

            if cur_window_index != active_window_index:
                # window changed so collect stats
                output_stat = self.get_stat(active_window_index, len(self.id_set))
                output_stat['time_elapsed_us'] = time_elapsed_us
                print(json.dumps(output_stat))
                window_stat_arr.append(output_stat)
                active_window_index = cur_window_index
        else:
            output_stat = self.get_stat(active_window_index, len(self.id_set)) 
            output_stat['time_elapsed_us'] = time_elapsed_us
            window_stat_arr.append(output_stat)
            active_window_index = cur_window_index
        
        if output_path is not None:
            with open(output_path, "w+") as f:
                json.dump(window_stat_arr, f, indent=4)
    