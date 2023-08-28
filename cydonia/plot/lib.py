import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt


def autolabel(ax, rects):
    """ Attach a text label above each bar displaying its height

        Code taken from: https://matplotlib.org/2.0.2/examples/api/barchart_demo.html
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 
                1.05*height,
                '%d' % int(height),
                ha='center', 
                va='bottom', 
                fontsize=8)


def plot_bars(ax, percentiles, vals):
    rects = ax.bar(percentiles, vals, width=8)
    autolabel(ax, rects)


def plot_iat_percentile_diff(block_trace_path, sample_trace_path, plot_path, percentiles=[1] + list(range(10,100,10))):
    original_df = pd.read_csv(block_trace_path, names=['ts', 'lba', 'op', 'size'])
    original_df['iat'] = original_df['ts'].diff()
    original_df.fillna(0, inplace=True)

    sample_df = pd.read_csv(sample_trace_path, names=['ts', 'lba', 'op', 'size'])
    sample_df['iat'] = sample_df['ts'].diff()
    sample_df.fillna(0, inplace=True)

    print("Loaded block {} and sample trace {}".format(len(original_df), len(sample_df)))

    fig, (ax, ax1, ax2) = plt.subplots(nrows=3, ncols=1)

    original_percentile_vals = np.percentile(original_df['iat'].to_numpy(), percentiles)
    sample_percentile_vals = np.percentile(sample_df['iat'].to_numpy(), percentiles)

    diff_index = 0 
    diff_array = np.zeros(len(original_percentile_vals), dtype=float)
    for orig_val, sample_val in zip(original_percentile_vals, sample_percentile_vals):
        if sample_val == 0 or orig_val == 0:
            diff_array[diff_index] = 0 
        else:
            diff_array[diff_index] = 100*(orig_val - sample_val)/orig_val
        diff_index += 1
    


    mean_original_iat = original_df.iloc[1:]['iat'].mean()
    plot_bars(ax, percentiles, original_percentile_vals)
    ax.set_xticks([])
    #ax.set_title("Mean: {:.3f}".format(mean_original_iat), fontsize=10, pad=5)

    mean_sample_iat = sample_df.iloc[1:]['iat'].mean()
    plot_bars(ax1, percentiles, sample_percentile_vals)
    ax1.set_xticks([])
    #ax1.set_title("Mean: {:.3f}".format(mean_sample_iat), fontsize=10, pad=5)

    mean_diff = 100*(mean_original_iat - mean_sample_iat)/mean_original_iat
    plot_bars(ax2, percentiles, diff_array)
    #ax2.set_title("Mean Error (%): {:.3f}".format(mean_diff), fontsize=10, pad=5)

    title_str = "Mean: sample={:.1f}, orig={:.1f}, error%={:.1f}".format(mean_sample_iat, mean_original_iat, mean_diff)

    for per, orig_val, sample_val, val in zip(percentiles, original_percentile_vals, sample_percentile_vals, diff_array):
        print("{}->{},{},{}".format(per, orig_val, sample_val, val))

    max_y_val = max(ax.get_ylim(), ax1.get_ylim())
    ax.set_ylim(max_y_val)
    ax1.set_ylim(max_y_val)

    ax2.set_ylabel("Error (%)")
    ax.set_ylabel("IAT ($\mu$s)")
    ax1.set_ylabel("IAT ($\mu$s)")

    ax2.set_xticks(percentiles, percentiles)
    ax2.set_xlabel("Percentiles")

    ax.set_title(title_str, fontsize=12, pad=10)

    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close(fig)


def plot_array_percentiles(ax, array, percentiles):
    percentile_vals = np.percentile(array, percentiles)
    ax.bar(len(percentile_vals), percentile_vals)


def plot_basic_graphs(block_trace_path, out_dir, file_extension='png', percentiles=range(0,100,5)):
    """ Plot a set of 'basic' graphs from a block storage trace

        Parameters
        ----------
        block_trace_path : pathlib.Path/str 
            path to block storage trace to generate plots from 
    """
    df = pd.read_csv(block_trace_path, names=['ts', 'lba', 'op', 'size'])
    df['iat'] = df['ts'].diff()

    # iat plot 
    plot_path = out_dir.joinpath("iat_bar")
    plot_path.mkdir(exist_ok=True, parents=True)
    plot_path = plot_path.joinpath('{}.{}'.format(block_trace_path.stem, file_extension))

    fig, ax = plt.subplots()
    percentile_vals = np.percentile(df.iloc[1:]['iat'].to_numpy(), percentiles)

    for per, val in zip(percentiles, percentile_vals):
        print("{}->{}".format(per, val))

    #ax.bar(range(len(percentile_vals)), percentile_vals)
    ax.set_title("Mean: {}, Max: {}".format())
    plot_array_percentiles(ax, df.iloc[1:]['iat'].to_numpy(), percentiles)
    plt.savefig(plot_path)
    plt.close(fig)