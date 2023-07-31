"""This class runs block trace replay."""

from argparse import ArgumentParser 
from pathlib import Path 

from cydonia.cachelib.ReplayConfig import ReplayConfig
from cydonia.cachelib.Runner import Runner 


T2_FILE_PATH = Path("~/nvm/disk.file")
BACKING_FILE_PATH = Path("~/disk/disk.file")
CACHEBENCH_BINARY_PATH = Path("~/disk/CacheLib/opt/cachelib/bin/cachebench").expanduser()

REPLAY_DATA_PATH = Path("/dev/shm/tracereplay/")
REPLAY_DATA_PATH.mkdir(exist_ok=True, parents=True)
POWER_OUT_FILE_PATH = REPLAY_DATA_PATH.joinpath("power.csv")
STDOUT_FILE_PATH = REPLAY_DATA_PATH.joinpath("stdout.dump")
STDERR_FILE_PATH = REPLAY_DATA_PATH.joinpath("stderr.dump")
CONFIG_FILE_PATH = REPLAY_DATA_PATH.joinpath("config.json")
SERVER_USAGE_FILE_PATH = REPLAY_DATA_PATH.joinpath("usage.csv")
STAT_FILE_PATH = REPLAY_DATA_PATH.joinpath("stat_0.out")
TS_STAT_FILE_PATH = REPLAY_DATA_PATH.joinpath("tsstat_0.out")


class TraceReplay:
    def __init__(self) -> None:
        self.clean_files()


    def clean_files(self) -> None:
        STDOUT_FILE_PATH.unlink(missing_ok=True)
        STDERR_FILE_PATH.unlink(missing_ok=True)
        SERVER_USAGE_FILE_PATH.unlink(missing_ok=True)
        STAT_FILE_PATH.unlink(missing_ok=True)
        TS_STAT_FILE_PATH.unlink(missing_ok=True)
    

    def run(self) -> int:
        runner = Runner()
        return_code = runner.run([str(CACHEBENCH_BINARY_PATH.absolute()), "--json_test_config", CONFIG_FILE_PATH.absolute()], 
                                    STDOUT_FILE_PATH, 
                                    STDERR_FILE_PATH,
                                    SERVER_USAGE_FILE_PATH,
                                    POWER_OUT_FILE_PATH)
        return return_code
        

def main(args):
    config_kwargs = {}
    if args.t2_size_mb > 0:
        config_kwargs["nvmCacheSizeMB"] = args.t2_size_mb
        config_kwargs["nvmCachePaths"] = [str(T2_FILE_PATH.expanduser())]
    
    config_kwargs["replayRate"] = args.replay_rate
    config = ReplayConfig([str(args.block_trace_path.expanduser())], 
                [str(BACKING_FILE_PATH.expanduser())], 
                args.t1_size_mb, 
                **config_kwargs)
    config.generate_config_file(CONFIG_FILE_PATH)

    replayer = TraceReplay()
    replayer.run()


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay using CacheBench.")

    parser.add_argument("block_trace_path",
        type=Path,
        help="Path of block trace file to replay.")
    
    parser.add_argument("t1_size_mb",
        type=int,
        help="Size of tier-1 cache in MB.")

    parser.add_argument("--t2_size_mb",
        type=int,
        default=0,
        help="Size of tier-2 cache in MB")
    
    parser.add_argument("--replay_rate",
        type=int,
        default=1,
        help="The factor by which interarrival times are divided to speed up replay.")
    
    args = parser.parse_args()

    main(args)