""" This script profiles block storage traces and outputs the features to a file. It currently supports 
    block storage traces with the format: timestamp(us), address, operation(read/write), size(bytes). 
"""

import argparse 
import pathlib 
import pandas as pd 

import logging
import logging.handlers as handlers

logger = logging.getLogger('profile_logger')
logger.setLevel(logging.INFO)

logHandler = handlers.RotatingFileHandler('/dev/shm/profile.log', maxBytes=25*1e6)
logHandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockTraceProfiler import BlockTraceProfiler


def workload_already_profiled(output_path, workload_name):
    """ Check if an entry with workload name exists in the output file """
    if not output_path.exists():
        return False 
    else:
        df = pd.read_csv(output_path)
        return len(df[df['workload_name']==workload_name]) > 0 


def main(block_trace_dir, output_path):
    """ Profile block traces in a directory and output its features to a file """
    for block_trace_path in sorted(block_trace_dir.iterdir(), key=lambda p: p.stat().st_size):
        workload_name = block_trace_path.stem 

        if workload_already_profiled(output_path, workload_name):
            logger.info("Done:{}".format(block_trace_path))
            continue 
        
        logger.info("Processing:{}".format(block_trace_path))
        reader = CPReader(block_trace_path)
        block_trace_profiler = BlockTraceProfiler(reader)
        block_trace_profiler.run()
        kwargs = {
            'workload_name': workload_name
        }
        block_trace_profiler.write_stat_to_file(output_path, **kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                description="Profile block storage traces and store the features in a file",
                formatter_class=argparse.RawDescriptionHelpFormatter,
                epilog="Notes:\n"
                        "* Example usage: python3 profile.py /home/block_trace_dir /home/features.csv")
    
    parser.add_argument("block_trace_dir",
        type=pathlib.Path,
        help="Directory with block storage traces")

    parser.add_argument("output_file",
        type=pathlib.Path,
        help="Path to file where features generated from profiling are stored")
    
    args = parser.parse_args()

    main(args.block_trace_dir, args.output_file)