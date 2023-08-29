"""CacheTraceProfiler generates features from a cache trace with format: 
timestamp, block_id, operation, reuse distance.

Usage:
    profiler = CacheTraceProfiler(block_trace_path)
    profiler.run()

    # get features
    features = profiler.get_features()

    # get rd arr 
    rd_arr = profiler.get_rd_arr()

    # write rd histogram to a file 
    profiler.create_rd_hist_file(rd_hist_file_path)

    # write features to a file 
    profiler.write_features_to_file(feature_file_path)
"""


from cydonia.profiler.CPReader import CPReader


class CacheTraceProfiler:
    def __init__(
            self, 
            block_trace_path: str 
    ) -> None:
        self.reader = CPReader(block_trace_path)
    

    def run(self):
        pass 
    
