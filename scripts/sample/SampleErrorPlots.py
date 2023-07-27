""" Generate plots comparing the difference in features between original and samples traces. """

import pathlib 
import argparse 
import pandas as pd 
import matplotlib.pyplot as plt

OUTPUT_DIR = pathlib.Path("/research2/mtc/cp_traces/sample/report")
FEATURE_DATA_DIR = pathlib.Path("./data/profile/cp-iat/")


class SampleErrorPlots:
    def __init__(self, data_dir, output_dir):
        self.data_dir = data_dir 
        self.sample_type = self.data_dir.name
        self.output_dir = output_dir.joinpath(self.sample_type)
    

    def plot_metric_vs_bits(self, workload_name, df, metric, plot_filename, x_label, y_label):
        sample_rate_line_type = {
            1: ['--', 'x'],
            5: ['-.', 'o'],
            10: [':', 's'],
            20: ['--', '8'],
            40: ['-.', 'p'],
            80: [':', 'v'],
            95: ['--', 'h']
        }
        fig, ax = plt.subplots(figsize=(14, 10))
        for group_rate, group_df in df.groupby(['rate']):
            if group_rate not in sample_rate_line_type:
                continue 
            rows = group_df[group_df['seed']>0]
            if len(rows) > 0:
                cur_style = sample_rate_line_type[group_rate]
                ax.plot(rows['bits'], 
                            rows[metric], 
                            marker=cur_style[1], 
                            linestyle=cur_style[0], 
                            linewidth=3,
                            markersize=15,
                            label=group_rate)

        ax.set_ylabel(y_label, fontsize=18)
        ax.set_xlabel(x_label, fontsize=18)
        ax.tick_params(axis='both', which='major', labelsize=18)

        plt.legend(fontsize=18)
        output_path = self.output_dir.joinpath(workload_name, plot_filename)
        output_path.parent.mkdir(exist_ok=True)
        plt.savefig(output_path)
        plt.close(fig)


    def generate_plots_from_sample(self, sample_data_path):
        workload_name = sample_data_path.stem
        df = pd.read_csv(sample_data_path)
        df = df.sort_values(by=['rate', 'bits'])
        full_row = df[df['seed']==0].iloc[0]

        df['read_size_diff'] = full_row['read_size_avg'] - df['read_size_avg'] 
        df['write_size_diff'] = full_row['write_size_avg'] - df['write_size_avg'] 
        df['iat_diff'] = full_row['iat_avg'] - df['iat_avg'] 

        metric = 'read_size_diff'
        avg_read_size_diff_plot_filename = "avg_{}_vs_bits.pdf".format(metric)
        y_label = 'Δ Read size (byte)'
        x_label = 'Number of lower order bits ignored'
        self.plot_metric_vs_bits(workload_name, df, metric, avg_read_size_diff_plot_filename, x_label, y_label)

        metric = 'write_size_diff'
        avg_write_size_diff_plot_filename = "avg_{}_vs_bits.pdf".format(metric)
        y_label = 'Δ Write size (byte)'
        x_label = 'Number of lower order bits ignored'
        self.plot_metric_vs_bits(workload_name, df, metric, avg_write_size_diff_plot_filename, x_label, y_label)

        metric = 'iat_diff'
        avg_iat_diff_plot_filename = "avg_{}_vs_bits.pdf".format(metric)
        y_label = 'Δ Inter-arrival time (\u03bcs)'
        x_label = 'Number of lower order bits ignored'
        self.plot_metric_vs_bits(workload_name, df, metric, avg_iat_diff_plot_filename, x_label, y_label)


    def generate(self):
        for sample_data_path in self.data_dir.iterdir():
            self.generate_plots_from_sample(sample_data_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Generate error plots for samples",
        epilog = "Example usage: python3 SampleErrorPlots.py")
    parser.add_argument("--data_path", 
                            type=pathlib.Path,
                            default=FEATURE_DATA_DIR,
                            help="Path to directory containing sample workload features")
    parser.add_argument("--output_dir",
        type = pathlib.Path,
        default = OUTPUT_DIR,
        help = "Directory to output files of the report")
    args = parser.parse_args()

    plotter = SampleErrorPlots(args.data_path, args.output_dir) 
    plotter.generate()