import unittest

import pandas as pd 
import numpy as np 
                                                    
from cydonia.cachelib.CleanRegions import CleanRegions


class TestCleanRegions(unittest.TestCase):
    def runTest(self):
        test_workload_s3_key = 'workloads/test/test_cp_workload.csv'
        t1_size_mb = 100
        t2_size_mb = 150 
        machine_specs = {
            "max_t1_size": 120,
            "max_t2_size": 470,
            "disk_file_path": "data/disk.file",
            "nvm_file_path": "/users/pbhandar/nvm/nvm.file",
            "block_trace_dir": "/dev/shm",
            "cachebench_config_path": "/dev/shm/cachebench_config.json",
            "exp_output_path": "/dev/shm/exp_output",
            "usage_output_path": "/dev/shm/exp_usage"
        }
        clean_region_list = [1, 10]

        experiment = CleanRegions(machine_specs, test_workload_s3_key, t1_size_mb, t2_size_mb, clean_region_list)
        experiment.run()
    
    


unittest.main()