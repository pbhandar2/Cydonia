""" Run all the experiments in the file "experiments.json". """
import os 
import json 
import argparse
import pathlib 
import socket 

from cydonia.util.S3Client import S3Client
from cydonia.cachelib.ReplayConfig import ReplayConfig
from cydonia.cachelib.Runner import Runner 

from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler


BACKING_FILE_PATH = pathlib.Path.home().joinpath("disk/disk.file")
NVM_FILE_PATH = pathlib.Path.home().joinpath("nvm/disk.file")
EXPERIMENT_FILE_PATH = pathlib.Path("experiments.json")
CACHEBENCH_BINARY_PATH = pathlib.Path.home().joinpath("disk/CacheLib/opt/cachelib/bin/cachebench")
OUTPUT_DIR = pathlib.Path("/dev/shm")
POWER_FILE_PATH = OUTPUT_DIR.joinpath("power.csv")

csv_handler = CSVHandler(str(POWER_FILE_PATH.absolute()))


class RunExperiment:
    def __init__(self, machine_name, experiment_file_path, backing_file_path, nvm_file_path, cachebench_binary_path, output_dir, num_itr):
        self.experiment_file_path = experiment_file_path
        with open(experiment_file_path) as f:
            self.experiment_list = json.load(f)

        self.backing_file_path = backing_file_path
        self.nvm_file_path = nvm_file_path
        self.cachebench_binary_path = cachebench_binary_path
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.config_file_path = self.output_dir.joinpath("config.json")
        self.exp_output_path = self.output_dir.joinpath("stdout.dump")
        self.power_usage_path = POWER_FILE_PATH
        self.usage_output_path = self.output_dir.joinpath("usage.csv")
        self.stat_file_path = self.output_dir.joinpath("stat_0.out")
        self.tsstat_file_path = self.output_dir.joinpath("tsstat_0.out")
        self.hostname = socket.gethostname()
        self.machine_name = machine_name
        self.num_itr = num_itr

        self.aws_key = os.environ['AWS_KEY']
        self.aws_secret = os.environ['AWS_SECRET']
        self.aws_bucket = os.environ['AWS_BUCKET']
        self.s3 = S3Client(self.aws_key, self.aws_secret, self.aws_bucket)


    def experiment_running(self, config, workload, cur_iteration):
       live_s3_key = self.get_s3_key("live", workload, config, cur_iteration)
       done_s3_key = self.get_s3_key("done", workload, config, cur_iteration)
       return self.s3.check_prefix_exist(live_s3_key) or self.s3.check_prefix_exist(done_s3_key)


    def get_s3_key(self, status, workload, config, cur_iteration):
        t2_size_mb = 0 
        if "nvmCacheSizeMB" in config["cache_config"]:
            t2_size_mb = config["cache_config"]["nvmCacheSizeMB"] if config["cache_config"]["nvmCacheSizeMB"] > 0 else 0
        
        queue_size = config["test_config"]['blockReplayConfig']['maxPendingBlockRequestCount']
        block_threads = config["test_config"]['blockReplayConfig']['blockRequestProcesserThreads']
        async_threads = config["test_config"]['blockReplayConfig']['asyncIOReturnTrackerThreads']
        replay_rate = config["test_config"]['blockReplayConfig']['replayRate']
        t1_size_mb = config['cache_config']["cacheSizeMB"]
        return "replay_files/{}/{}/{}/q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(status,
                                                                                    self.machine_name,
                                                                                    workload,
                                                                                    queue_size, 
                                                                                    block_threads,
                                                                                    async_threads,
                                                                                    t1_size_mb, 
                                                                                    t2_size_mb,
                                                                                    replay_rate,
                                                                                    cur_iteration)
    
    @measure_energy(handler=csv_handler)
    def _run(self):
        
        runner = Runner()
        return_code = runner.run(self.cachebench_binary_path, 
                                    self.config_file_path, 
                                    self.exp_output_path, 
                                    self.usage_output_path)
        return return_code


    def run(self):
        for cur_iteration in range(self.num_itr):
            for experiment_entry in self.experiment_list:
                t1_size_mb, trace_s3_key = experiment_entry['t1_size_mb'], experiment_entry['trace_s3_key']

                kwargs = {}
                if "nvmCacheSizeMB" in experiment_entry["kwargs"]:
                    kwargs["nvmCacheSizeMB"] = experiment_entry["kwargs"]["nvmCacheSizeMB"]
                    kwargs["nvmCachePaths"] = [str(self.nvm_file_path.absolute())]
                
                if "replayRate" in experiment_entry["kwargs"]:
                    kwargs["replayRate"] = experiment_entry["kwargs"]["replayRate"]
                
                workload = pathlib.Path(experiment_entry["trace_s3_key"]).stem 
                local_trace_path = self.output_dir.joinpath("{}.csv".format(workload))
                config = ReplayConfig([str(local_trace_path.resolve())], 
                                        [str(self.backing_file_path.resolve())], 
                                        experiment_entry["t1_size_mb"], 
                                        **kwargs)
                config.generate_config_file(self.config_file_path)

                if self.experiment_running(config.get_config(), workload, cur_iteration):
                    print("Done-> Experiment {},{} already done", config.get_config(), cur_iteration)
                    continue 
                
                print("Running-> Experiment {},{}", config.get_config(), cur_iteration)
                live_s3_key_prefix = self.get_s3_key("live", workload, config.get_config(), cur_iteration)
                self.s3.upload_s3_obj("{}/{}".format(live_s3_key_prefix, self.config_file_path.name), str(self.config_file_path.absolute()))
                self.s3.download_s3_obj(experiment_entry["trace_s3_key"], str(local_trace_path.absolute()))

                return_code = self._run()
                csv_handler.save_data()
                print("Completed-> Experiment {},{} with return code {}", config.get_config(), cur_iteration, return_code)
                if return_code == 0:
                    done_s3_key_prefix = self.get_s3_key("done", workload, config.get_config(), cur_iteration)
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.config_file_path.name), str(self.config_file_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.exp_output_path.name), str(self.exp_output_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.usage_output_path.name), str(self.usage_output_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.stat_file_path.name), str(self.stat_file_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.tsstat_file_path.name), str(self.tsstat_file_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.power_usage_path.name), str(self.power_usage_path.absolute()))
                    # delete the key in the live link we uploaded to signify this experiment is running
                    self.s3.delete_s3_obj("{}/{}".format(live_s3_key_prefix, self.config_file_path.name))
                    print("Done-> Experiment {},{}", config.get_config(), cur_iteration)
                else:
                    error_s3_key_prefix = self.get_s3_key("error", workload, config.get_config(), cur_iteration)
                    self.s3.upload_s3_obj("{}/{}".format(error_s3_key_prefix, self.config_file_path.name), str(self.config_file_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(error_s3_key_prefix, self.exp_output_path.name), str(self.exp_output_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(error_s3_key_prefix, self.usage_output_path.name), str(self.usage_output_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(error_s3_key_prefix, self.stat_file_path.name), str(self.stat_file_path.absolute()))
                    self.s3.upload_s3_obj("{}/{}".format(error_s3_key_prefix, self.tsstat_file_path.name), str(self.tsstat_file_path.absolute()))
                    print("Error-> Experiment {},{}", config.get_config(), cur_iteration)
                    # delete the key in the live link we uploaded to signify this experiment is running
                    self.s3.delete_s3_obj("{}/{}".format(live_s3_key_prefix, self.config_file_path.name))
                    return 


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run block storage replay for all configurations listed in the experiment file.")

    parser.add_argument("machine_name",
                            help="Name used to uniquly identify a experiment.")

    parser.add_argument("--experiment_file", 
                            default=EXPERIMENT_FILE_PATH,
                            type=pathlib.Path,
                            help="Experiment file path")
    
    parser.add_argument("--backing_file_path", 
                            default=BACKING_FILE_PATH, 
                            type=pathlib.Path, 
                            help="Path to file on backing storage")

    parser.add_argument("--nvm_file_path", 
                            default=NVM_FILE_PATH,
                            type=pathlib.Path, 
                            help="Path to file on NVM device")

    parser.add_argument("--cachebench_binary_path", 
                            default=CACHEBENCH_BINARY_PATH,
                            type=pathlib.Path, 
                            help="Path to the cachebench binary")

    parser.add_argument("--output_dir", 
                            default=OUTPUT_DIR,
                            type=pathlib.Path, 
                            help="Directory where all files related to experiment is stored")
    
    parser.add_argument("--num_iteration",
                            default=3,
                            type=int,
                            help="The number of iterations to run experiments.")
    
    args = parser.parse_args()

    runner = RunExperiment(args.machine_name,
                            args.experiment_file, 
                            args.backing_file_path, 
                            args.nvm_file_path,
                            args.cachebench_binary_path,
                            args.output_dir,
                            args.num_iteration)
    runner.run()