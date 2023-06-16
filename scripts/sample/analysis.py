import argparse 
import pathlib 


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Analyze samples")
    parser.add_argument("trace_path", type=pathlib.Path, help="Path to block trace")
    parser.add_argument("-s", '--samples', nargs='+', type=str, help='Samples to analyze')
    args = parser.parse_args()

    print(args)

    