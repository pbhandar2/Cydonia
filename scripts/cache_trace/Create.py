from pathlib import Path 
from argparse import ArgumentParser

from cydonia.profiler.CPReader import CPReader


class CreateCacheTrace:
    def __init__(self, block_trace_path: Path):
        self._block_trace_path = block_trace_path
        self._reader = CPReader(str(block_trace_path))
    

    def create(self, cache_trace_path: Path):
        self._reader.generate_cache_trace(cache_trace_path)


def main():
    parser = ArgumentParser(description="Create a cache trace from a block trace.")
    parser.add_argument("block_trace_path", type=Path, help="Path of block trace file.")
    parser.add_argument("cache_trace_path", type=Path, help="Path of cache trace file.")
    args = parser.parse_args()

    cache_trace = CreateCacheTrace(args.block_trace_path)
    cache_trace.create(args.cache_trace_path)