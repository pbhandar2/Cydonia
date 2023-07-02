import pathlib 
import argparse 

from cydonia.sample.SampleReport import SampleReport 

MAX_BITS_IGNORE = 12
SAMPLE_DIR = pathlib.Path("/research2/mtc/cp_traces/sample/block")
ORIGINAL_DIR = pathlib.Path("/research2/mtc/cp_traces/csv_traces")
OUTPUT_DIR = pathlib.Path("/research2/mtc/cp_traces/sample/report")
SAMPLE_RATE_LIST = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]


class Report:
    def __init__(self, sample_type, out_dir):
        self.sample_type = sample_type 
        self.out_dir = out_dir.joinpath(sample_type)
        self.out_dir.mkdir(exist_ok=True, parents=True)

        self.default_bit_list = list(range(MAX_BITS_IGNORE))
        self.default_sample_rate_list = SAMPLE_RATE_LIST
        

    def get_workload_name_from_sample(self, sample_file_name):
        split_sample_file_name = sample_file_name.split("_")
        return split_sample_file_name[0]


    def generate_report(self, sample_dir, original_dir):
        for sample_trace_path in sample_dir.joinpath(self.sample_type).iterdir():
            print("Evaluating : {}".format(sample_trace_path))
            workload_name = self.get_workload_name_from_sample(sample_trace_path.stem)
            original_trace_path = original_dir.joinpath("{}.csv".format(workload_name))
            report = SampleReport(original_trace_path, sample_trace_path, self.out_dir)
            report.generate_report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Generate report from samples",
        epilog = "Example usage: python3 Report.py --sample_type iat2 ")
    
    parser.add_argument("--sample_type",
        type = str,
        default = "cp-iat",
        choices = ["cp-iat", "cp-iat2"],
        help = "The type of sample")

    parser.add_argument("--sample_dir",
        type = pathlib.Path,
        default = SAMPLE_DIR,
        help = "Directory containing the samples")

    parser.add_argument("--original_dir",
        type = pathlib.Path,
        default = ORIGINAL_DIR,
        help = "Directory containing the full original trace")

    parser.add_argument("--output_dir",
        type = pathlib.Path,
        default = OUTPUT_DIR,
        help = "Directory to output files of the report")

    parser.add_argument("--rate",
        nargs = "*",
        type = float,
        default = SAMPLE_RATE_LIST,
        help = "Sampling rate")

    parser.add_argument("--bits",
        nargs="*",
        type=str,
        default=list(range(MAX_BITS_IGNORE)),
        help="Number of lower order bits ignored during sampling")
    
    parser.add_argument("--seed",
        type=int,
        default=42,
        help="Random seed")

    args = parser.parse_args()

    report = Report(args.sample_type, args.output_dir)
    report.generate_report(args.sample_dir, args.original_dir)