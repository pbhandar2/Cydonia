import copy 
import time
import numpy as np 
from collections import Counter, defaultdict 

from cydonia.profiler.PercentileStats import PercentileStats


""" BlockWorkloadStats
    ------------------
    This class generates the read and write statistics related to 
    frequency, IO size, sequentiality, alignment and more.  

    Parameters
    ----------
    lba_size : int (Optional)
        size of a logical block address in bytes (Default: 512)
    page_size : int (Optional)
        size of a page in bytes (Default: 4096)
"""
# TODO: rename class to BlockCacheStat, since the features are more 
# about a block cache, than just the block workload
class BlockWorkloadStats:

    def __init__(self, lba_size=512, page_size=4096):
        self._lba_size = lba_size 
        self._page_size = page_size 

        self._read_block_req_count = 0 
        self._read_page_access_count = 0 
        self._read_io_request_size_sum = 0
        self._read_seq_count = 0 
        self._read_misalignment_sum = 0 
        self._min_read_page = 0 
        self._max_read_page = 0
        self._read_page_access_counter = Counter()

        self._write_block_req_count = 0 
        self._write_page_access_count = 0 
        self._write_io_request_size_sum = 0 
        self._write_seq_count = 0 
        self._write_misalignment_sum = 0 
        self._min_write_page = 0 
        self._max_write_page = 0
        self._write_page_access_counter = Counter()

        self._read_size_pstats = PercentileStats()
        
        self._write_size_pstats = PercentileStats()
        
        self._jump_distance_pstats = PercentileStats()
        
        self._scan_pstats = PercentileStats()
        
        self._iat_pstats = PercentileStats()
        

        self._prev_req = None 
        self._scan_length = 0 
        self._start_time = time.time()


    def block_req_count(self):
        """ This functions returns the number of block requests. """
        return self._read_block_req_count + self._write_block_req_count


    def write_block_req_split(self):
        """ This function returns the fraction of block requests that were writes. """
        return 0 if self.block_req_count()==0 else self._write_block_req_count/self.block_req_count()


    def io_request_size_sum(self):
        """ This function returns the total IO in bytes. """
        return self._read_io_request_size_sum + self._write_io_request_size_sum 


    def write_io_request_size_split(self):
        """ This function returns the fraction of IO that was for write requests. """
        return 0 if self.io_request_size_sum()==0 else self._write_io_request_size_sum/self.io_request_size_sum()


    def page_access_count(self):
        """ This function returns the number of pages accessed. """
        return self._read_page_access_count + self._write_page_access_count


    def write_page_access_split(self):
        """ This function returns the fraction of write page requests. """
        return 0 if self.page_access_count()==0 else self._write_page_access_count/self.page_access_count()

    
    def seq_count(self):
        """ This functions returns the number of sequential block accesses. """
        return self._read_seq_count + self._write_seq_count


    def write_seq_split(self):
        """ This function returns the number of sequential block accesses that were writes. """
        return 0 if self.seq_count()==0 else self._write_seq_count/self.seq_count()


    def range(self):
        """ This functions returns the byte range accessed in a workload. """
        return self._page_size * (max(self._max_read_page, self._max_write_page) \
                    - min(self._min_read_page, self._min_write_page))


    def read_range(self):
        """ This functions returns the byte range read in a workload. """
        return self._page_size * (self._max_read_page - self._min_read_page)


    def write_range(self):
        """ This functions returns the byte range read in a workload. """
        return self._page_size * (self._max_write_page - self._min_write_page)
    

    def misalignment_sum(self):
        """ This functions returns the total byte misalignment in block requests. """
        return self._read_misalignment_sum + self._write_misalignment_sum
    

    def req_start_offset(self, req):
        """ This function returns the start offset of a block request. """
        return req["lba"] * self._lba_size


    def req_end_offset(self, req):
        """ This function returns the end offset of a block request. """
        return self.req_start_offset(req) + req["size"]

    
    def page_working_set_size(self):
        """ This function returns the size of the working set size. """
        read_wss = set(self._read_page_access_counter.keys())
        write_wss = set(self._write_page_access_counter.keys())
        return self._page_size * len(read_wss.union(write_wss))


    def read_page_working_set_size(self):
        """ This function returns the size of the read working set size. """
        return self._page_size * len(self._read_page_access_counter.keys())


    def write_page_working_set_size(self):
        """ This function returns the size of the write working set size. """
        return self._page_size * len(self._write_page_access_counter.keys())

    
    def write_page_working_set_size_split(self):
        """ This function returns the fraction of working set size that was written upon. """
        return 0 if self.page_working_set_size()==0 else self.write_page_working_set_size()/self.page_working_set_size()

    
    def read_page_popularity_map(self):
        """ This function returns a map of pages read to its popularity. """
        return self._get_popularity_map(self._read_page_access_counter, self._read_page_access_count)


    def write_page_popularity_map(self):
        """ This function returns a map of pages written to its popularity. """
        return self._get_popularity_map(self._write_page_access_counter, self._write_page_access_count)


    def _get_popularity_map(self, counter, total):
        popularity_map = defaultdict(float)
        for key in counter.keys():
            popularity_map[key] = counter[key]/total
        return popularity_map


    def _get_popularity_percentile(self, counter, total):
        popularity_map = self._get_popularity_map(counter, total)
        popularity_stat = PercentileStats(size=len(popularity_map.keys()))
        sum_total = 0.0 
        for page_key in popularity_map:
            popularity_stat.add_data(popularity_map[page_key])
            sum_total += (popularity_map[page_key] * page_key)
        return popularity_stat
        

    def _get_popularity_change_percentile(self, cur_map, prev_map):
        prev_window_key_set = set(prev_map.keys())
        cur_window_key_set = set(cur_map)
        final_key_set = set.union(cur_window_key_set, prev_window_key_set)
        popularity_stat = PercentileStats(size=len(final_key_set))
        for page_key in final_key_set:
            popularity_stat.add_data(cur_map[page_key]-prev_map[page_key])
        return popularity_stat


    def _track_req_alignment(self, req):
        """ This function tracks the byte alignment of block requests. 

            Parameters
            ----------
            req : object 
                an object containing block request features
        """

        if req["op"] == "r":
            self._read_misalignment_sum += req["front_misalign"]
            self._read_misalignment_sum += req["rear_misalign"]
        elif req["op"] == "w":
            self._write_misalignment_sum += req["front_misalign"]
            self._write_misalignment_sum += req["rear_misalign"]


    def _track_seq_access(self, req):
        """ A sequential block request starts at the same 
            offset where the previous block request ended. 

            The operation type (read/write) of a sequential 
            access is determined by the following request
            and the operation of the previous request is 
            irrelevant. 

            Parameters
            ----------
            req : object 
                an object containing block request features
        """

        if self._prev_req == None:
            return 

        """ We are comparing the start offset of current request 
            to the end offset of previous request. """
        start_offset = self.req_start_offset(req)
        prev_end_offset = self.req_end_offset(self._prev_req)
        self._jump_distance_pstats.add_data(start_offset-prev_end_offset)
        
        if start_offset == prev_end_offset:
            # Sequential! 
            if req["op"] == 'r':
                self._read_seq_count += 1 
            else:
                self._write_seq_count += 1
        

    def _track_op_type(self, req):
        """ This function tracks the request counts for read and write. 

            Parameters
            ----------
            req : object 
                an object containing block request features
        """

        if req["op"] ==  'r':
            self._read_block_req_count += 1
            self._read_io_request_size_sum += req["size"]
            self._read_page_access_count += req["end_page"] - req["start_page"] + 1
            self._min_read_page = max(self._min_read_page, req["start_page"])
            self._max_read_page = max(self._max_read_page, req["end_page"])
            self._read_size_pstats.add_data(req["size"])
            
        elif req["op"] == 'w':
            self._write_block_req_count += 1 
            self._write_io_request_size_sum += req["size"]
            self._write_page_access_count += req["end_page"] - req["start_page"] + 1
            self._min_write_page = max(self._min_write_page, req["start_page"])
            self._max_write_page = max(self._max_write_page, req["end_page"])
            self._write_size_pstats.add_data(req["size"])
            
        else:
            raise ValueError("Operation {} not supported. Only 'r' or 'w'".format(req["op"]))


    def _track_popularity(self, req):
        """ Change in popularity of an item. 

            Parameters
            ----------
            req : object 
                an object containing block request features
        """

        if req['op'] == 'r': 
            for page_index in range(req["start_page"], req["end_page"]+1):
                if page_index not in self._read_page_access_counter:
                    self._scan_length += 1 
                else:
                    if self._scan_length > 0:
                        self._scan_pstats.add_data(self._scan_length)
                        
                        self._scan_length = 0 
                self._read_page_access_counter[page_index] += 1
        else:
            for page_index in range(req["start_page"], req["end_page"]+1):
                self._write_page_access_counter[page_index] += 1
                self._scan_length += 1

    
    def _track_iat(self, req):
        """ Track the interarrival time of block requests. 

            Parameters
            ----------
            req : object 
                an object containing block request features
        """

        if self._prev_req is not None:
            self._iat_pstats.add_data(req["ts"] - self._prev_req["ts"])

    
    def get_stat(self):
        """ Get features of the workload as a dictionary. 

            Return 
            ------
            stat : dictionary 
                dictionary of features and values 
        """
        stat = {}

        stat['block_req_count'] = self.block_req_count()
        stat['read_block_req_count'] = self._read_block_req_count
        stat['write_block_req_count'] = self._write_block_req_count
        stat['write_block_req_split'] = self.write_block_req_split()

        stat['cache_req_count'] = self.page_access_count()
        stat['read_cache_req_count'] = self._read_page_access_count
        stat['write_cache_req_count'] = self._write_page_access_count
        stat['write_cache_req_split'] = self.write_page_access_split()

        stat['io_req_byte'] = self.io_request_size_sum()
        stat['read_io_req_byte'] = self._read_io_request_size_sum
        stat['write_io_req_byte'] = self._write_io_request_size_sum
        stat['write_io_req_split'] = self.write_io_request_size_split()

        stat['seq_count'] = self.seq_count()
        stat['read_seq_count'] = self._read_seq_count
        stat['write_seq_count'] = self._write_seq_count
        stat['write_seq_split'] = self.write_seq_split()

        stat['misalignment_byte'] = self.misalignment_sum()
        stat['read_misalignment_byte'] = self._read_misalignment_sum
        stat['write_misalignment_byte'] = self._write_misalignment_sum
        
        stat['range'] = self.range()
        stat['min_offset'] = min(self._min_read_page, self._min_write_page) * self._page_size

        stat['wss'] = self.page_working_set_size()
        stat['read_wss'] = self.read_page_working_set_size()
        stat['write_wss'] = self.write_page_working_set_size()
        stat['write_wss_split']= self.write_page_working_set_size_split()

        read_popularity_stat = self._get_popularity_percentile(self._read_page_access_counter, self._read_page_access_count)
        for percentile, percentile_val in zip(read_popularity_stat.percentiles_tracked, read_popularity_stat.get_percentiles()):
            stat['read_page_popularity_p{}'.format(percentile)] = percentile_val
        stat['read_page_popularity_avg'] = read_popularity_stat.get_mean()

        write_popularity_stat = self._get_popularity_percentile(self._write_page_access_counter, self._write_page_access_count)
        for percentile, percentile_val in zip(write_popularity_stat.percentiles_tracked, write_popularity_stat.get_percentiles()):
            stat['write_page_popularity_p{}'.format(percentile)] = percentile_val
        stat['write_page_popularity_avg'] = write_popularity_stat.get_mean()

        for percentile, percentile_val in zip(self._read_size_pstats.percentiles_tracked, self._read_size_pstats.get_percentiles()):
            stat['read_size_p{}'.format(percentile)] = percentile_val
        stat['read_size_avg'] = self._read_size_pstats.get_mean()

        for percentile, percentile_val in zip(self._write_size_pstats.percentiles_tracked, self._write_size_pstats.get_percentiles()):
            stat['write_size_p{}'.format(percentile)] = percentile_val
        stat['write_size_avg'] = self._write_size_pstats.get_mean()

        for percentile, percentile_val in zip(self._jump_distance_pstats.percentiles_tracked, self._jump_distance_pstats.get_percentiles()):
            stat['jump_distance_p{}'.format(percentile)] = percentile_val
        stat['jump_distance_avg'] = self._jump_distance_pstats.get_mean()

        for percentile, percentile_val in zip(self._scan_pstats.percentiles_tracked, self._scan_pstats.get_percentiles()):
            stat['scan_p{}'.format(percentile)] = percentile_val
        stat['scan_avg'] = self._scan_pstats.get_mean()

        for percentile, percentile_val in zip(self._iat_pstats.percentiles_tracked, self._iat_pstats.get_percentiles()):
            stat['iat_p{}'.format(percentile)] = percentile_val
        stat['iat_avg'] = self._iat_pstats.get_mean()

        return stat


    def add_request(self, block_req):
        """ Update the statistics based on a block request 
            provided by the user. 

            Parameters
            ----------
            block_req : dict 
                dict containing block request features """

        self._track_op_type(block_req)
        self._track_seq_access(block_req)
        self._track_req_alignment(block_req)
        self._track_popularity(block_req)
        self._track_iat(block_req)
        self._prev_req = block_req