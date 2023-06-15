""" This class represents a configuration file needed to run block trace replay in CacheBench. """


import json 
import socket 
import pathlib 


class ReplayConfig:
    def __init__(self, machine_id, machine_config_json_file):
        self.queue_size = 128 
        self.thread_count = 16 
        self.replay_rate = 1 

        # update LRU list on a write request 
        self.lru_update_on_write = True 

        # min alloc size that worked for page size of 4096 so metadata is 40 bytes 
        self.alloc_size = 4136 

        # tier-2 cache configuration 
        self.t2_admission_rate = 0.0 
        self.max_device_write_rate_mb = 0
        self.clean_region_size_mb = 16 
        self.num_clean_region = 1 

        # get the specs of this machine 
        self.machine_id = machine_id
        with open(machine_config_json_file, 'r') as f:
            self.all_machine_config = json.load(f)
            self.machine_specs = self.all_machine_config[machine_id]
        
        # unique identifier of the machine 
        self.tag = socket.gethostname()
    

    def generate_config_file(self, t1_size_mb, t2_size_mb, **kwargs):
        """ Generate a file with a configuration that can be used for block 
            replay. 

            Parameters
            ----------
            t1_size_mb : int 
                the size of tier-1 cache in MB 
            t2_size_mb : int 
                the size of tier-2 cache in MB
            **kwargs : dict 
                a dict with custom configurations 
            
            Return 
            ------
            config : dict 
                the cachebench replay configuration dict 
        """
        config = {
            "cache_config": {},
            "test_config": {}
        }
        config["cache_config"]["cacheSizeMB"] = t1_size_mb
        config["cache_config"]["lruUpdateOnWrite"] = self.lru_update_on_write
        config["cache_config"]["allocSizes"] = [self.alloc_size]

        queue_size = self.queue_size if 'queue_size' not in kwargs else kwargs['queue_size']
        thread_count = self.thread_count if 'thread_count' not in kwargs else kwargs['thread_count']
        replay_rate = self.replay_rate if 'replay_rate' not in kwargs else kwargs['replay_rate']

        if t2_size_mb > 0:
            config["cache_config"]["nvmCacheSizeMB"] = t2_size_mb
            config["cache_config"]["nvmCachePaths"] = self.machine_specs["nvm_file_path"]
            config["cache_config"]["navySizeClasses"] = []
            config["cache_config"]["navyBigHashSizePct"] = 0
            config["cache_config"]["navyBlockSize"] = 4096
            config["cache_config"]["truncateItemToOriginalAllocSizeInNvm"] = True 
            config["cache_config"]["printNvmCounters"] = True 

            assert  not ('max_t2_write_rate_mb' in kwargs and 't2_admission_rate' in kwargs),
                "Cannot use max device write rate and t2 admission rate at the same time"

            if 'max_t2_write_rate_mb' in kwargs:
                config["cache_config"]["navyMaxDeviceWriteRateMB"] = kwargs["max_t2_write_rate_mb"]

            if 't2_admission_rate' in kwargs:
                config["cache_config"]["navyAdmissionProbability"] = kwargs['t2_admission_rate']
            
            if 'num_clean_region' in kwargs:
                config["cache_config"]["navyCleanRegions"] = kwargs['num_clean_region']
            
        config["test_config"]["populateItem"] = True 
        config["test_config"]["generator"] = "multi-replay"
        config["test_config"]["numThreads"] = 1
        config["test_config"]["inputQueueSize"] = queue_size
        config["test_config"]["processorThreadCount"] = thread_count
        config["test_config"]["asyncIOTrackerThreadCount"] = thread_count
        config["test_config"]["traceBlockSizeBytes"] = 512
        config["test_config"]["pageSizeBytes"] = 4096
        config["test_config"]["statPrintDelaySec"] = 30
        config["test_config"]["relativeTiming"] = True 
        config["test_config"]["scaleIAT"] = replay_rate
        config["test_config"]["diskFilePath"] = self.machine_specs["disk_file_path"]
        config["test_config"]["maxDiskFileOffset"] = pathlib.Path(self.machine_specs["disk_file_path"]).expanduser().stat().st_size

        block_trace_dir = pathlib.Path(self.machine_specs["block_trace_dir"])
        num_traces = 1 if 'workload_count' not in kwargs else kwargs['workload_count']

        trace_list = []
        for trace_index in range(kwargs['workload_count']):
            trace_list.append(str(block_trace_dir.joinpath("block_trace_{}.csv".format(trace_index)))
        config["test_config"]["replayGeneratorConfig"] = {
            "traceList": trace_list
        }
        
        config["test_config"]["tag"] = self.tag

        with open(self.machine_specs["cachebench_config_path"], "w+") as f:
            f.write(json.dump(config, indent=4))
        
        return config