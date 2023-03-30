""" This script runs experiment for a given machine, workload and tier sizes while 
varying the number of clean regions. 
"""
import json 
import pathlib 

from cydonia.runner.Runner import Runner 
from cydonia.runner.ReplayConfig import ReplayConfig

class CleanRegions:
    def __init__(self, machine_specs, block_trace_s3_key, t1_size_mb, t2_size_mb, clean_regions_list):
        self.machine_specs = machine_specs 
        self.block_trace_s3_key = block_trace_s3_key
        self.t1_size_mb = t1_size_mb
        self.t2_size_mb = t2_size_mb
        self.clean_regions_list = clean_regions_list
        self.block_trace_path = pathlib.Path(self.machine_specs["block_trace_dir"]).joinpath("block_trace.csv")
        self.config = ReplayConfig(self.block_trace_path, 
                                    self.machine_specs["nvm_file_path"], 
                                    self.machine_specs["disk_file_path"],
                                    self.t1_size_mb, 
                                    self.t2_size_mb, 
                                    0)
        self.runner = Runner()
    

    def run(self):
        """ Run the CleanRegions experiments. 
        """
        for clean_regions in self.clean_regions_list:
            config_file_path = pathlib.Path(self.machine_specs["cachebench_config_path"])
            self.config.generate_config_file(config_file_path)

            # run the experiment 
            # self.runner("../../../../opt/bin/cachebench", 
            #                 config_file_path, 
            #                 self.machine_specs["exp_output_path"],
            #                 self.machine_specs["ss"])
            # cleanup the output and prepare to run another one 

            print("running clean {}".format(clean_regions))
            print(config_file_path)
            