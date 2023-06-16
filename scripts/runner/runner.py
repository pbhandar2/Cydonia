import argparse
import pathlib 

from cydonia.cachelib.ReplayConfig import ReplayConfig
from cydonia.cachelib.Runner import Runner 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a cachebench block trace replay")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to block trace file")
    parser.add_argument("t1_size_mb", type=int, help="Size of tier-1 cache in MB")
    parser.add_argument("t2_size_mb", type=int, help="Size of tier-2 cache in MB")
    parser.add_argument("backing_file_path", type=pathlib.Path, help="Path to file on backing storage")
    args = parser.parse_args()

    config_path = pathlib.Path("/dev/shm/config.json")
    cachebench_binary_path = pathlib.Path.home().joinpath("disk/CacheLib/opt/cachelib/bin/cachebench")
    experiment_output_dir= pathlib.Path("/dev/shm/out.dump")
    usage_output_dir = pathlib.Path("/dev/shm/usage.csv")

    config = ReplayConfig([str(args.trace_path.resolve())], [str(args.backing_file_path.resolve())], args.t1_size_mb)
    config.generate_config_file(config_path)

    runner = Runner()
    runner.run(cachebench_binary_path, config_path, experiment_output_dir, usage_output_dir)