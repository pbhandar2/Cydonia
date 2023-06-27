import pathlib 
import argparse 

import cydonia
import cydonia.plot 
from cydonia.plot.lib import plot_basic_graphs, plot_iat_percentile_diff

class Plot:
    def __init__(self, sample_type):
        self.plot_dir = pathlib.Path("/research2/mtc/cp_traces/sample_plots").joinpath(sample_type)
        self.plot_dir.mkdir(exist_ok=True, parents=True)

        self.original_dir = pathlib.Path("/research2/mtc/cp_traces/csv_traces/")

        self.sample_dir = pathlib.Path("/research2/mtc/cp_traces/sample/").joinpath(sample_type)
        assert self.sample_dir.exists(), "Sample directory does not exist {}".format(self.sample_dir)


    def run(self):
        for sample_trace_path in self.sample_dir.iterdir():
            print("Plotting trace path: {}".format(sample_trace_path))
            sample_file_name = sample_trace_path.stem 
            workload_name = sample_file_name.split("_")[0]
            original_trace_path = self.original_dir.joinpath("{}.csv".format(workload_name))
            plot_dir = self.plot_dir.joinpath(workload_name)
            plot_dir.mkdir(exist_ok=True, parents=True)
            plot_path = plot_dir.joinpath("{}.png".format(sample_file_name))
            plot_iat_percentile_diff(original_trace_path, sample_trace_path, plot_path)
            print("Generated: {}".format(plot_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate plots from samples",
        epilog="Example usage: python3 Plot.py")
    parser.add_argument("sample_type",
                            help="The sample type to identify the directory of samples to evaluate")
    args = parser.parse_args()

    plotter = Plot(args.sample_type)
    plotter.run()