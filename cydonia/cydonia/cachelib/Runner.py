""" This class runs a CacheBench experiment with a given configuration 
    file. The output of the experiment and the resource (memory,CPU)
    usage of the experiment are logged to files specified by the user. 

    Attributes
    ----------
    snapshot_window_seconds : int (Optional)(Default: 300)
        user specified window size in seconds to snap memory and CPU usage 
"""

import pathlib 
import subprocess
import psutil 
import time 
import sys 
import pandas as pd 


class Runner:
    def __init__(self, snapshot_window_seconds=300):
        """
            Parameters
            ----------
            snapshot_window_seconds : int 
                user specified window size in seconds to snap memory and CPU usage 
        """
        self.snapshot_window_seconds = snapshot_window_seconds


    def snap_system_stats(self, output_path):
        """ Write memory and CPU stats to a file. 

            Parameters
            ----------
            usage_handle : int 
                the handle of the file where memory and CPU usage are logged 
        """
        mem_stats = psutil.virtual_memory()

        out_dict = {}
        for mem_stat_field in mem_stats._fields:
            mem_stat_value = getattr(mem_stats, mem_stat_field)
            if type(mem_stat_value) == str:
                out_dict["mem_{}".format(mem_stat_field)] = mem_stat_value

        cpu_stats = psutil.cpu_percent(percpu=True)
        for cpu_index, cpu_stat in enumerate(cpu_stat_name):
            out_dict["cpu_{}".format(cpu_index)] = cpu_stat

        df = pd.DataFrame([out_dict])
        if output_path.exists():
            old_df = pd.read_csv(output_path)
            df = pd.concat([old_df, df], ignore_index=True)
        print(df)
        df.to_csv(output_path, index=False)


    def run(self, cachebench_binary_path, cachebench_config_file_path, experiment_output_path, resource_usage_output_path):
        """ Run the CacheBench experiment and track its memory and CPU 
            usage. 

            Parameters
            ----------
            cachebench_binary_path : pathlib.Path
                path to the cachebench binary 
            cachebench_config_file_path : pathlib.Path
                path to JSON file containing CacheBench configuration 
            experiment_output_path : pathlib.Path 
                path to the directory where stdout is dumped 
            resource_usage_output_path : pathlib.Path 
                path to the directory where the resource usage file is stored 
            
            Return 
            ------
            return_code : int 
                the status of experiment process on termination 
        """
        return_code = -1 
        start_time = time.time()
        with experiment_output_path.open("w+") as output_handle, \
                resource_usage_output_path.open("w+") as usage_handle:

            # run the cachelib experiment 
            with subprocess.Popen([cachebench_binary_path, 
                                    "--json_test_config", 
                                    str(cachebench_config_file_path)], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.STDOUT, 
                                    bufsize=1,
                                    encoding='utf-8') as process_handle:

                while process_handle.poll() is None:
                    time.sleep(self.snapshot_window_seconds)
                    self.snap_system_stats(resource_usage_output_path)
                            
                # wait for it to complete 
                process_handle.wait()
                return_code = process_handle.returncode
        
        return return_code