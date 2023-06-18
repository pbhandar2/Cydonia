import argparse
import pathlib 

from cydonia.cachelib.ReplayConfig import ReplayConfig
from cydonia.cachelib.Runner import Runner 

DEFAULT_BACKING_FILE_PATH = pathlib.Path.home().joinpath("disk/disk.file")
DEFAULT_NVM_FILE_PATH = pathlib.Path.home().joinpath("nvm/disk.file")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a cachebench block trace replay")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to block trace file")
    parser.add_argument("t1_size_mb", type=int, help="Size of tier-1 cache in MB")
    parser.add_argument("t2_size_mb", type=int, help="Size of tier-2 cache in MB")
    parser.add_argument("--backing_file_path", 
                            default=DEFAULT_BACKING_FILE_PATH, 
                            type=pathlib.Path, 
                            help="Path to file on backing storage")
    parser.add_argument("--nvm_file_path", 
                            default=DEFAULT_NVM_FILE_PATH,
                            type=pathlib.Path, 
                            help="Path to file on NVM device")
    args = parser.parse_args()

    config_path = pathlib.Path("/dev/shm/config.json")
    cachebench_binary_path = pathlib.Path.home().joinpath("disk/CacheLib/opt/cachelib/bin/cachebench")
    experiment_output_dir= pathlib.Path("/dev/shm/out.dump")
    usage_output_dir = pathlib.Path("/dev/shm/usage.csv")

    kwargs = {}
    if args.t2_size_mb > 0:
        kwargs["nvmCacheSizeMB"] = args.t2_size_mb
        kwargs["nvmCachePaths"] = [str(args.nvm_file_path.absolute())]

    config = ReplayConfig([str(args.trace_path.resolve())], [str(args.backing_file_path.resolve())], args.t1_size_mb, **kwargs)
    config.generate_config_file(config_path)



    print(config)

    #runner = Runner()
    #runner.run(cachebench_binary_path, config_path, experiment_output_dir, usage_output_dir)