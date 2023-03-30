import unittest

import pandas as pd 
import numpy as np 
                                                    
from cydonia.runer.CleanRegions import CleanRegions


class TestCleanRegions(unittest.TestCase):
    def runTest(self):
        test_workload_s3_key = 'workloads/test/test_cp_workload.csv'
        t1_size_mb = 100
        t2_size_mb = 150 
        experiment = CleanRegions()
        experiment.run()

    
    


unittest.main()