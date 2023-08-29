from pathlib import Path 
from pandas import DataFrame 
from itertools import product
from unittest import main, TestCase

from cydonia.sample.Sample import sample 


class TestSampler(TestCase):
    test_block_trace_path = Path("../data/test_cp.csv")
    test_sample_block_dir_path = Path("../data/test_samples/")

    def test_sample_size(self):
        self.test_sample_block_dir_path.mkdir(exist_ok=True)

        seed = 42
        bits_arr = [0, 1, 2, 4, 8]
        rate_arr = [1, 5, 10, 20, 40, 80]
        sample_stats_arr = []
        for rate, bits in product(rate_arr, bits_arr):
            sample_file_name = "{}_{}_{}.csv".format(rate, bits, seed)
            print("Generating test sample {}".format(sample_file_name))
            sample_stats = sample(self.test_block_trace_path, rate/100, seed, bits, self.test_sample_block_dir_path.joinpath(sample_file_name))
            sample_stats_arr.append(sample_stats)
        
        sample_df = DataFrame(sample_stats_arr)
        for group_index, group_df in sample_df.groupby(by=['rate']):
            print(group_index)
            print(group_df)


if __name__ == '__main__':
    main()