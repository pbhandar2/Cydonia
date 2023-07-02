""" This class profiles samples and full block storage traces and stores the features in CSV files. 
"""
import pathlib 
import argparse 
import textwrap
import pandas as pd 

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockTraceProfiler import BlockTraceProfiler


class Profiler:
    def __init__(self, original_trace_path_list, sample_trace_path_list, output_path):
        self.original_trace_path_list = original_trace_path_list
        self.sample_trace_path_list = sample_trace_path_list
        self.output_path = output_path 
        self.output_path.parent.mkdir(exist_ok=True)
        self.output_df = None 
        if self.output_path.is_file():
            self.output_df = pd.read_csv(output_path)
    

    def workload_already_profiled(self, workload_info):
        """ Check if a workload is already profiled

            Parameters
            ----------
            workload_info : dict 
                dictionary containing information about the workload such as name, rate, seed, bits 
            
            Return 
            ------
            profiled_flag : bool
                flag indicating whether workload has already been profiled
        """
        output_path = self.output_path
        output_df = self.output_df 
        if self.output_path.is_dir():
            output_path = self.output_path.joinpath("{}.csv".format(workload_info['workload']))
            if output_path.is_file():
                output_df = pd.read_csv(output_path)

        print(output_df)
        if not output_path.exists() or output_df is None:
            return False
        else:
            return len(output_df[(output_df["workload"] == workload_info["workload"]) &
                                    (output_df["rate"] == workload_info["rate"]) &
                                    (output_df["seed"] == workload_info["seed"]) &
                                    (output_df["bits"] == workload_info["bits"])]) > 0
    

    def profile(self):
        """ Profile all relevant sample, original block storage trace pair 
        """
        for original_trace_path in self.original_trace_path_list:
            workload_name = original_trace_path.stem 
            original_kwargs = {
                "rate": 0,
                "seed": 0,
                "bits": 0,
                "workload": original_trace_path.stem 
            }
            for sample_trace_path in self.sample_trace_path_list:
                split_sample_trace_path = sample_trace_path.stem.split("_")
                sample_workload_name = split_sample_trace_path[0]

                if sample_workload_name != workload_name:
                    continue 

                if not self.workload_already_profiled(original_kwargs):
                    reader = CPReader(original_trace_path)
                    block_trace_profiler = BlockTraceProfiler(reader)
                    block_trace_profiler.run()
                    
                    output_path = self.output_path
                    if output_path.is_dir():
                        output_path = self.output_path.joinpath("{}.csv".format(workload_name))
                    
                    block_trace_profiler.write_stat_to_file(output_path, **original_kwargs)
                    print("Processed->{}, recorded in {}".format(original_trace_path, output_path))

                kwargs = {
                    "workload": sample_workload_name,
                    "rate": split_sample_trace_path[1],
                    "seed": split_sample_trace_path[2],
                    "bits": split_sample_trace_path[3]
                }
                if self.workload_already_profiled(kwargs):
                    print("Done->{}".format(sample_trace_path))
                    continue 

                reader = CPReader(sample_trace_path)
                block_trace_profiler = BlockTraceProfiler(reader)
                block_trace_profiler.run()

                output_path = self.output_path
                if output_path.is_dir():
                    output_path = self.output_path.joinpath("{}.csv".format(workload_name))
                
                block_trace_profiler.write_stat_to_file(output_path, **kwargs)
                print("Processed->{}, recorded in {}".format(sample_trace_path, output_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Profile sample and its corresponding full block straoge trace",
                                        formatter_class=argparse.RawDescriptionHelpFormatter,
                                        epilog="Notes:\n"
                                                "* Example usage: python3 Profiler.py ~/original_trace.csv ~/samples/ ~/sample_data.csv\n"
                                                "* Sample file name should have the format: $WORKLOAD_NAME$_$REPLAY_RATE$_$SEED$_$BITS$.csv\n"
                                                "* The corresponding full trace file name of each sample has the format: $WORKLOAD_NAME$.csv\n"
                                                "* This workload name of sample and original trace is matched to ensured correct files are used\n"
                                                "* The output file name has the format: $WORKLOAD_NAME$.csv\n")

    parser.add_argument("original_trace_path",
                            type=pathlib.Path,
                            help="Path to a full trace or a directory containing full traces")

    parser.add_argument("sample_trace_path", 
                            type=pathlib.Path,
                            help="Path to a sample or a directory containing samples")

    parser.add_argument("output_path",
                            type=pathlib.Path,
                            help="Output path of file with workload feature or directory to output such files")

    args = parser.parse_args()

    sample_trace_path_list = []
    if args.sample_trace_path.is_dir():
        sample_trace_path_list = list(args.sample_trace_path.iterdir())
    else:
        sample_trace_path_list = [args.sample_trace_path]

    original_trace_path_list = []
    if args.original_trace_path.is_dir():
        original_trace_path_list = list(args.original_trace_path.iterdir())
    else:
        original_trace_path_list = [args.original_trace_path]
    
    profiler = Profiler(original_trace_path_list, sample_trace_path_list, args.output_path)
    profiler.profile()