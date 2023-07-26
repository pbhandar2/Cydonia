import json 
import copy 
import pandas as pd 
from pathlib import Path

from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
csv_handler = CSVHandler("mt_energy.csv")

class MTExperiments:
    def __init__(self) -> None:
        self.workloads = ["w09", "w18", "w64", "w66", "w92"]
        self.wss_to_size_percent = [0.1, 0.2, 0.4, 0.6]
        self.block_df = pd.read_csv("~/disk/blocks.csv")
        self.output_file_path = Path("files/MTExperiments.json")

    
    def generate_experiments_for_workload(
        self, 
        workload: str,
        replay_rate_list: list = [8, 4, 2, 1]
    ) -> None:
        row = self.block_df[self.block_df["workload_name"] == workload].iloc[0]
        wss = row['wss']
        config_list = []
        for replay_rate in replay_rate_list:
            for t1_size_percent in self.wss_to_size_percent:
                t1_size = wss * t1_size_percent
                
                st_config = {
                    "t1_size_mb": int(t1_size/(1024**2)),
                    "trace_s3_key": "workloads/cp/{}.csv".format(workload),
                    "kwargs": {
                        "replayRate": replay_rate
                    }
                }
                config_list.append(st_config)

                for t2_size_percent in self.wss_to_size_percent:
                    t2_size = wss * t2_size_percent

                    mt_config = {
                        "t1_size_mb": int(t1_size/(1024**2)),
                        "trace_s3_key": "workloads/cp/{}.csv".format(workload),
                        "kwargs": {
                            "nvmCacheSizeMB": int(t2_size/(1024**2)),
                            "replayRate": replay_rate
                        }
                    }
                    config_list.append(mt_config)

        return config_list 


    @measure_energy(handler=csv_handler)
    def generate_experiment_file(self) -> None:
        config_list = []
        for workload in self.workloads:
            config_list += self.generate_experiments_for_workload(workload)
        
        with open(self.output_file_path, "w+") as out_handle:
            json.dump(config_list, out_handle, indent=4)

if __name__ == "__main__":
    mt_experiment = MTExperiments()
    mt_experiment.generate_experiment_file()