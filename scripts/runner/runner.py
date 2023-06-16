import pathlib 

from cydonia.cachelib.ReplayConfig import ReplayConfig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a cachebench block trace replay")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to block trace file")
    parser.add_argument("t1_size_mb", type=int, help="Size of tier-1 cache in MB")
    parser.add_argument("t2_size_mb", type=int, help="Size of tier-2 cache in MB")
    parser.add_argument("backing_file_path", type=pathlib.Path, help="Path to file on backing storage")
    args = parser.parse_args()

    config = ReplayConfig([args.trace_path], args.t1_size_mb, [args.backing_file_path])