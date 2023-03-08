""" This class contains the configurations needed to run 
    a block replay experiment. 

    Attributes
    ----------
    block_trace_path : pathlib.Path 
        path to the block trace to replay 
    disk_file_path : pathlib.Path 
        path to disk file where IO requests are made 
    t2_file_path : pathlib.Path 
        path to file on the device which will be used as a tier-2 cache 
    t1_size_mb : int 
        the size of tier-1 cache in MB 
    t2_size_mb : int 
        the size of tier-2 cache in MB 
    it : int 
        iteration count of the experiment 
    queue_size : int (Default: 128)
        the application queue size 
    thread_count : int (Default: 16)
        the number of threads used to process storage IO request to the system and async IO to backing store 
    replay_rate : int (Default: 1)
        the replay rate (1 means same as original)
    t2_admit_rate : float (Default: 0)
        tier-2 cache admission rate 
    t2_max_device_write_rate_mb : int (Default: 0)
        maximum write rate to tier-2 device in MB
    num_clean_region : int (Default: 1)
        number of clean regions in tier-2 cache 
    region_size_mb : int (Default: 16)
        the size of region in MB 
    accelerate_replay_flag : bool (Default: True)
        does not follow timestamps if system is idle when set to True 
    tag : str (Default: socket.gethostname())
        a string to identify the specific machine 
"""


import json 
import socket 
import pathlib 


class ReplayConfig:
    def __init__(self, block_trace_path, t2_file_path, disk_file_path, t1_size_mb, t2_size_mb, it):
        self.block_trace_path = pathlib.Path(block_trace_path) 
        self.t2_file_path = pathlib.Path(t2_file_path) 
        self.disk_file_path = pathlib.Path(disk_file_path)

        self.t1_size_mb = t1_size_mb 
        self.t2_size_mb = t2_size_mb 
        self.it = it 

        # default params 
        self.queue_size = 128 
        self.thread_count = 16 
        self.replay_rate = 1 
        self.t2_admit_rate = 0.0 
        self.t2_max_device_write_rate_mb = 0 
        self.num_clean_region = 1 
        self.region_size_mb = 16 
        self.accelerate_replay_flag = True 
        self.cachelib_min_t1_size_mb = 100 
        self.cachelib_min_t2_size_mb = 150
        self.tag = socket.gethostname()
    

    def generate_config_file(self, config_file_path):
        """ Generate a JSON configuration file based on the attributes of this 
            class. 

            Parameters
            ----------
            config_file_path : pathlib.Path/str 
                path to the configuration file 
        """
        config = self.get_base_config()
        with config_file_path.open('w+') as f:
            f.write(json.dumps(config, indent=4))


    def get_base_config(self):
        """ Get the dict needed to run block trace replay with the configurations set in this 
            class. 

            Return 
            ------
            config : dict 
                the dict with basic experiment configuration needed to sucessfully run an experiment 
        """
        config = {
            "cache_config": {},
            "test_config": {}
        }
        config["cache_config"]["cacheSizeMB"] = self.t1_size_mb
        config["cache_config"]["lruUpdateOnWrite"] = True 
        config["cache_config"]["allocSizes"] = [4136]

        if t2_size > 0:
            config["cache_config"]["nvmCacheSizeMB"] = self.t2_size_mb
            config["cache_config"]["nvmCachePaths"] = str(self.t2_file_path)
            config["cache_config"]["navySizeClasses"] = []
            config["cache_config"]["navyBigHashSizePct"] = 0
            config["cache_config"]["navyBlockSize"] = 4096
            config["cache_config"]["truncateItemToOriginalAllocSizeInNvm"] = True 
            config["cache_config"]["printNvmCounters"] = True 
            config["cache_config"]["navyCleanRegions"] = self.num_clean_region
            config["cache_config"]["navyRegionSizeMB"] = self.region_size_mb
            config["cache_config"]["navyAdmissionProbability"] = self.t2_admit_rate 
            config["cache_config"]["navyMaxDeviceWriteRateMB"] = self.t2_max_device_write_rate_mb
            
        config["test_config"]["populateItem"] = True 
        config["test_config"]["generator"] = "multi-replay"
        config["test_config"]["numThreads"] = 1
        config["test_config"]["inputQueueSize"] = self.queue_size
        config["test_config"]["processorThreadCount"] = self.thread_count
        config["test_config"]["asyncIOTrackerThreadCount"] = self.thread_count
        config["test_config"]["traceBlockSizeBytes"] = 512
        config["test_config"]["pageSizeBytes"] = 4096
        config["test_config"]["statPrintDelaySec"] = 30
        config["test_config"]["relativeTiming"] = True 
        config["test_config"]["scaleIAT"] = self.replay_rate
        config["test_config"]["diskFilePath"] = str(self.disk_file_path)
        config["test_config"]["maxDiskFileOffset"] = pathlib.Path(self.disk_file_path.expanduser().stat().st_size
        config["test_config"]["replayGeneratorConfig"] = {
            "traceList": [str(self.block_trace_path)]
        }
        config["test_config"]["tag"] = self.tag
        return config 