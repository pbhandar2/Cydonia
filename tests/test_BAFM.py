from pathlib import Path 
from unittest import main, TestCase
from itertools import product

from cydonia.profiler.BAFM import BAFM
from cydonia.profiler.CacheTrace import CacheTraceReader


def eval_bafm(lower_addr_bits_ignored: int, num_iter: int) -> None:
    print("Testing {} and {}".format(lower_addr_bits_ignored, num_iter))

    # input files 
    test_data_dir = Path("../data")
    cache_trace_path = Path("../data/test_cp_cache.csv")
    
    # files created
    output_file_path = Path("../data/bafm_output.csv")
    block_access_file_path = test_data_dir.joinpath("access_{}.csv".format(lower_addr_bits_ignored))

    if output_file_path.exists():
        output_file_path.unlink()
    
    if block_access_file_path.exists():
        block_access_file_path.unlink()

    # test if writing one BAFM to file and loading it in another BAFM
    # produces identical BAFMs
    bafm = BAFM(lower_addr_bits_ignored)
    bafm.load_cache_trace(cache_trace_path)
    bafm.write_map_to_file(block_access_file_path)

    new_bafm = BAFM(lower_addr_bits_ignored)
    new_bafm.load_block_access_file(block_access_file_path)
    assert bafm == new_bafm, "The two BAFMs are not equal."

    # test if removing N blocks and writing the output to a file
    # and loading a new BAFM using that output file produces
    # identical BAFMs
    cache_trace = CacheTraceReader(cache_trace_path)
    workload_stats = cache_trace.get_stat()
    cache_trace.close()

    bafm.remove_n_blocks(workload_stats, workload_stats, "mean", num_iter, output_file_path)
    new_bafm.update_state(output_file_path, workload_stats)
    assert bafm == new_bafm, "The two BAFMs are not equal."
    output_file_path.unlink()
    block_access_file_path.unlink()


class TestBAFM(TestCase):
    def test_access_feature(self):
        num_lower_addr_bits_arr = [0, 1, 2]
        num_iter_arr = [2, 5, 10, 20, 50, 100, 200, 500, 1000]

        for num_lower_addr_bits, num_iter in product(num_lower_addr_bits_arr, num_iter_arr):
             eval_bafm(num_lower_addr_bits, num_iter)


if __name__ == '__main__':
    main()