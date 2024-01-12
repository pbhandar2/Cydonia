from unittest import main, TestCase
from pathlib import Path 

from cydonia.profiler.BlockTrace import BlockTrace
from cydonia.profiler.CacheTrace import CacheTraceReader


class TestCacheTrace(TestCase):
    def test_CacheTrace(self):
        test_data_dir = Path("../data")
        test_cache_trace_path = test_data_dir.joinpath("test_cp_cache.csv")
        test_block_trace_path = test_data_dir.joinpath("test_cp.csv")
        test_hash_file_path_1 = test_data_dir.joinpath("hash1.csv")
        test_hash_file_path_2 = test_data_dir.joinpath("hash2.csv")
        test_block_stat_file_path = test_data_dir.joinpath("stat.json")
        
        cache_reader = CacheTraceReader(test_cache_trace_path)
        stats = cache_reader.get_stat()
        stats.write_to_file(test_block_stat_file_path)

        cache_reader.create_sample_hash_file(42, 0, test_hash_file_path_1)
        cache_reader.create_sample_hash_file(42, 1, test_hash_file_path_2)

        block_trace = BlockTrace(test_block_trace_path)
        block_stats = block_trace.get_block_stat()

        assert stats == block_stats

        cache_reader.close()


if __name__ == '__main__':
    main()