from argparse import ArgumentParser
from pathlib import Path 


def main():
    parser = ArgumentParser(description="Use BlkSample algorithm to reduce sample to improve feature accuracy.")
    parser.add_argument("workload", type=str, help="Name of the workload.")
    parser.add_argument("`")
    args = parser.parse_args()

if __name__ == "__main__":
    main()