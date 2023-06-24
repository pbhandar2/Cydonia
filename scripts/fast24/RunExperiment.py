""" Run all the experiments in the file "experiments.json". """
import os 
import json 
import argparse
import pathlib 
import socket 

from cydonia.util.S3Client import S3Client
from cydonia.cachelib.ReplayConfig import ReplayConfig
from cydonia.cachelib.Runner import Runner 


BACKING_FILE_PATH = pathlib.Path.home().joinpath("disk/disk.file")
NVM_FILE_PATH = pathlib.Path.home().joinpath("nvm/disk.file")
EXPERIMENT_FILE_PATH = pathlib.Path("experiments.json")
CACHEBENCH_BINARY_PATH = pathlib.Path.home().joinpath("disk/CacheLib/opt/cachelib/bin/cachebench")
OUTPUT_DIR = pathlib.Path("/dev/shm")


class RunExperiment:
    def __init__(self, machine_type, experiment_file_path, backing_file_path, nvm_file_path, cachebench_binary_path, output_dir):
        self.experiment_file_path = experiment_file_path
        with open(experiment_file_path) as f:
            self.experiment_list = json.load(f)

        self.backing_file_path = backing_file_path
        self.nvm_file_path = nvm_file_path
        self.cachebench_binary_path = cachebench_binary_path
        self.output_dir = output_dir.joinpath("cachebench")
        self.output_dir.mkdir(exist_ok=True)
        self.config_file_path = self.output_dir.joinpath("config.json")
        self.exp_output_path = self.output_dir.joinpath("stdout.dump")
        self.usage_output_path = self.output_dir.joinpath("usage.csv")
        self.stat_file_path = self.output_dir.joinpath("stat_0.out")
        self.tsstat_file_path = self.output_dir.joinpath("tsstat_0.out")
        self.hostname = socket.gethostname()
        self.machine_type = machine_type
        self.machine_name = self.hostname.split(".")[0]
        self.iteration_count = 3 

        self.aws_key = os.environ['AWS_KEY']
        self.aws_secret = os.environ['AWS_SECRET']
        self.aws_bucket = os.environ['AWS_BUCKET']
        self.s3 = S3Client(self.aws_key, self.aws_secret)


    def experiment_running(self, config, workload, cur_iteration):
       live_s3_key = self.get_s3_key("live", workload, config, cur_iteration)
       done_s3_key = self.get_s3_key("done", workload, config, cur_iteration)
       return self.s3.check_prefix_exist(live_s3_key) or self.s3.check_prefix_exist(done_s3_key)


    def get_s3_key(self, status, workload, config, cur_iteration):
        t2_size_mb = 0 
        if "nvmCacheSizeMB" in config["cache_config"]:
            t2_size_mb = config["cache_config"]["nvmCacheSizeMB"]
        
        queue_size = config["test_config"]['blockReplayConfig']['maxPendingBlockRequestCount']
        block_threads = config["test_config"]['blockReplayConfig']['blockRequestProcesserThreads']
        async_threads = config["test_config"]['blockReplayConfig']['asyncIOReturnTrackerThreads']
        t1_size_mb = config['cache_config']["cacheSizeMB"]
        return "replay/{}/{}/{}/q={}_bt={}_at={}_t1={}_t2={}_it={}".format(status,
                                                                                self.machine_type,
                                                                                workload,
                                                                                queue_size, 
                                                                                block_threads,
                                                                                async_threads,
                                                                                t1_size_mb, 
                                                                                t2_size_mb,
                                                                                cur_iteration)
    

    def run(self):
        for experiment_entry in self.experiment_list:
            t1_size_mb, trace_s3_key = experiment_entry['t1_size_mb'], experiment_entry['trace_s3_key']

            kwargs = {}
            if "nvmCacheSizeMB" in experiment_entry["kwargs"]:
                kwargs["nvmCacheSizeMB"] = experiment_entry["kwargs"]["nvmCacheSizeMB"]
                kwargs["nvmCachePaths"] = [str(self.nvm_file_path.absolute())]
            
            workload = pathlib.Path(experiment_entry["trace_s3_key"]).stem 
            local_trace_path = self.output_dir.joinpath("{}.csv".format(workload))

            config = ReplayConfig([str(local_trace_path.resolve())], 
                                    [str(self.backing_file_path.resolve())], 
                                    experiment_entry["t1_size_mb"], 
                                    **kwargs)
            config.generate_config_file(self.config_file_path)

            for cur_iteration in range(self.iteration_count):
                if self.experiment_running(config.get_config(), workload, cur_iteration):
                    print("Done-> Experiment {},{} already done", config.get_config(), cur_iteration)
                    continue 
                
                print("Running-> Experiment {},{}", config.get_config(), cur_iteration)
                live_s3_key_prefix = self.get_s3_key("live", workload, config.get_config(), cur_iteration)
                self.s3.upload_s3_obj("{}/{}".format(live_s3_key_prefix, self.config_file_path.name), str(self.config_file_path.absolute()))
                self.s3.download_s3_obj(experiment_entry["trace_s3_key"], str(local_trace_path.absolute()))

                runner = Runner()
                runner.run(self.cachebench_binary_path, 
                            self.config_file_path, 
                            self.exp_output_path, 
                            self.usage_output_path)

                print("Completed-> Experiment {},{}", config.get_config(), cur_iteration)
                # upload all necessary files 
                done_s3_key_prefix = self.get_s3_key("live", workload, config.get_config(), cur_iteration)
                self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.config_file_path.name), str(self.config_file_path.absolute()))
                self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.exp_output_path.name), str(self.experiment_output_path.absolute()))
                self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.usage_output_path.name), str(self.usage_output_path.absolute()))
                self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.stat_file_path.name), str(self.stat_file_path.absolute()))
                self.s3.upload_s3_obj("{}/{}".format(done_s3_key_prefix, self.tsstat_file_path.name), str(self.tsstat_file_path.absolute()))

                # clean output directory by removing all files 
                for path in self.output_dir.iterdir():
                    path.unlink()

                # delete the key in the live link we uploaded to signify this experiment is running
                self.s3.delete_s3_obj("{}/{}".format(live_s3_key_prefix, self.config_file_path.name))
                print("Done-> Experiment {},{}", config.get_config(), cur_iteration)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all the experiment listed in the experiment file.")

    parser.add_argument("--machine_type".
                            help="The type of machine")

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
    
    args = parser.parse_args()

    runner = RunExperiment(args.machine_type,
                            args.experiment_file, 
                            args.backing_file_path, 
                            args.nvm_file_path,
                            args.cachebench_binary_path,
                            args.output_dir)
    runner.run()