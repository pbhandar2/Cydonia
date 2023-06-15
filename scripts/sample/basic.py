import argparse 
import pathlib 

from cydonia.sample.Sampler import generate_samples

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Sample a block storage trace")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to block trace")
    parser.add_argument("seed", type=int, help="random_seed")
    parser.add_argument("ts_method", type=str, default="iat", help="Method to generate sample timestamps (iat or ts)")
    parser.add_argument("out_dir", type=pathlib.Path, help="The directory to output samples")
    parser.add_argument("-r", '--rate', nargs='+', type=float, help='Sampling rates')
    parser.add_argument('-b','--bits', nargs='*', type=int, help='Bits to ignore')
    args = parser.parse_args()

    generate_samples(args.trace_path, args.rate, args.seed, args.bits, args.ts_method, args.out_dir)