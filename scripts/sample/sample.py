import argparse 
import pathlib 

from cydonia.sample.Sampler import Sampler

def getPercent(count, total):
    return int(100*count/total)


def get_output_file_path(out_dir, bits, rate, seed, ts_method, multi_sample_percent):
    if bits is None:
        bits_str = 'NA'
    else:
        bits_str = "-".join([str(_) for _ in args.bits])

    out_file_name = "{}_{}_{}_{}_{}.csv".format(ts_method, int(100*rate), seed, multi_sample_percent, bits_str)
    return out_dir.joinpath(out_file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Generate samples from a block storage trace")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to a block storage trace")
    parser.add_argument("seed", type=int, help="Random seed")
    parser.add_argument("ts_method", type=str, default="iat", help="Method to generate sample timestamps (iat or ts)")
    parser.add_argument("out_dir", type=pathlib.Path, help="Directory for output files")
    # need at least one sampling rate 
    parser.add_argument("-r", '--rate', nargs='+', type=float, help='Sampling rates')
    # if no arguments provided, do not ignore any bits 
    parser.add_argument('-b','--bits', nargs='*', type=int, help='Bits to ignore')
    args = parser.parse_args()

    sampler = Sampler(args.trace_path, bits=args.bits)
    for rate in args.rate:
        sample_df, sampled_block_req_count = sampler.sample(rate, args.seed, args.ts_method)
        split_block_req_count = len(sample_df) - sampled_block_req_count
        split_block_req_percent = getPercent(split_block_req_count, len(sample_df))
        
        workload_name = args.trace_path.stem 
        out_sub_dir = args.out_dir.joinpath(workload_name)
        out_sub_dir.mkdir(exist_ok=True)

        out_path = get_output_file_path(out_sub_dir, 
                                            args.bits, 
                                            rate, 
                                            args.seed, 
                                            args.ts_method, 
                                            split_block_req_percent)

        sample_df.to_csv(out_path, index=False, header=False)
        print("Sample output path: {}".format(out_path))