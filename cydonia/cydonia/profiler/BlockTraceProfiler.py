import pandas as pd 
import numpy as np 
import pathlib 
import copy 
import logging
from collections import defaultdict

from cydonia.profiler.BlockWorkloadStats import BlockWorkloadStats

""" BlockTraceProfiler
    ------------------
    This class profiles a block trace and generates features. 

    Parameters
    ----------
    reader : Reader
        a reader that returns cache requests in order 
    stat_types : list
        list of strings indicating what types of features to generate 
"""
class BlockTraceProfiler:

    def __init__(self, reader, stat_types=['block'], **kwargs):
        self._page_size = 4096
        self._reader = reader 
        self._workload_name = self._reader.trace_file_path.stem 

        self._stat = {} # current stat
        if 'block' in stat_types:
            self._stat['block'] = BlockWorkloadStats()

        self._cur_req = {}
        self._cur_page_index = -1 
        self._time_elasped = 0 
        self._prev_req = {}


    def _load_next_block_req(self):
        """ This function returns the next block request from the reader. """
        self._cur_req = self._reader.get_next_block_req(page_size=self._page_size) 
        if self._cur_req:
            self._time_elasped = self._cur_req["ts"]
            self._cur_req["key"] = self._cur_req["start_page"]

            if "block" in self._stat:
                self._stat['block'].add_request(self._cur_req)


    def _load_next_cache_req(self):
        """ This function returns the next cache request. """
        self._prev_req = self._cur_req
        if not self._cur_req:
            self._load_next_block_req()
        else:
            if self._cur_req["key"] == self._cur_req["end_page"]:
                self._load_next_block_req()
            else:
                self._cur_req["key"] += 1
                if "rd" in self._stat:
                    self._cur_req["rd"] = self._rd_tracker.get_next_rd()


    def write_stat_to_file(self, output_path, **kwargs):
        """ Write the workload statistics as a row in a CSV file. 

            Parameters
            ----------
            output_path : pathlib.Path
                path of CSV files where each row represents a set of features 
            kwargs : dict 
                dict of additional information to include along with stats 
        """

        stat = self._stat['block'].get_stat()
        for key in kwargs:
            stat[key] = kwargs[key]

        df = pd.DataFrame([stat])
        if output_path.exists():
            cur_df = pd.read_csv(output_path)
            df = pd.concat([cur_df, df], ignore_index=True)
            df.to_csv(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)
            

    def run(self):
        """ This function computes features from the provided trace. """
        self._load_next_cache_req()
        while (self._cur_req):
            self._load_next_cache_req()