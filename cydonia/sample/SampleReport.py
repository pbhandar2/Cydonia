""" Take a sample and generate a comprehensive set of statistics while comparing it to the original. """
import pathlib 
import pandas as pd 

from cydonia.plot.lib import plot_sample_org_and_diff


class SampleReport:
    def __init__(self, original_trace_path, sample_trace_path, out_dir):
        self.sample_df = pd.read_csv(sample_trace_path, names=["ts", "lba", "op", "size"])
        self.sample_df['iat'] = self.sample_df['ts'].diff()
        self.sample_df.fillna(0, inplace=True)

        self.original_df = pd.read_csv(original_trace_path, names=["ts", "lba", "op", "size"])
        self.original_df['iat'] = self.original_df['ts'].diff()
        self.original_df.fillna(0, inplace=True)

        self.out_dir = out_dir.joinpath(sample_trace_path.stem)
        self.out_dir.mkdir(exist_ok=True, parents=True)
    

    def generate_report(self):
        # plot IAT percentiles
        iat_percentile_plot_file_name = 'iat_percentile.pdf'
        plot_sample_org_and_diff(self.original_df['iat'].to_numpy(),
                                    self.sample_df['iat'].to_numpy(),
                                    "IAT",
                                    self.out_dir.joinpath(iat_percentile_plot_file_name),
                                    unit="$\mu$s")

        # plot read size percentiles 
        read_size_plot_file_name = 'read_size.pdf'
        plot_sample_org_and_diff(self.original_df[self.original_df['op']=='r']['size'].to_numpy(),
                                    self.sample_df[self.sample_df['op']=='r']['size'].to_numpy(),
                                    "Size",
                                    self.out_dir.joinpath(read_size_plot_file_name),
                                    unit="byte")

        # plot write size percentiles 
        write_size_plot_file_name = 'write_size.pdf'
        plot_sample_org_and_diff(self.original_df[self.original_df['op']=='w']['size'].to_numpy(),
                                    self.sample_df[self.sample_df['op']=='w']['size'].to_numpy(),
                                    "Size",
                                    self.out_dir.joinpath(write_size_plot_file_name),
                                    unit="byte")