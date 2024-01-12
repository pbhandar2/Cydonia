# Cydonia

Cydonia is a package for analysis and sampling of block storage traces. Any trace with the following properties timestamp, address, operation (read/write) and size in bytes can be used
with Cydonia. 


## Analysis

Cydonia generated a large set of features given a block storage trace. The following are some of the features computed by Cydonia:

- Overall workload features: The overall features of the workload such as percentiles of read/write request sizes, inter-arrival time and more. 
- Per block access type features: A map with block addresses as key and array of block features based on access types. The access types is divided by the index of a block in a multi-block request. A block can be solo, left-most, right-most or middle block of a block request that can request accesses to multiple blocks.  


## Sampling

Cydonia uses randomized spatial sampling used in the FAST 15 paper ["Efficient MRC Construction with SHARDS"](https://www.usenix.org/system/files/conference/fast15/fast15-paper-waldspurger.pdf) to sample block addresses. Miss ratio curves (MRCs) generated from block addresses sampled using randomized spatial sampling have shown to have an average miss ratio error of less than 0.01 when using sampling rates lower than 1%[1].


### Multi-block requests
The above mentioned approach uses fixed sized blocks but block storage traces contain multi-block requests.
A multi-block request could fragment into multiple samples block requests. We tackle this by ignoring some bits in the addresses to sample group of blocks rather than individual blocks. Below is an example:

```
from cydonia.profiler.CPReader import CPReader 
from cydonia.profiler.CacheTrace import CacheTraceReader, HashFile

block_trace_path = "block_trace.csv"
block_reader = CPReader(block_trace_path)
# generate a cache trace with fixed sized block accesses 
cache_trace_path = "cache_trace.csv"
block_reader.generate_cache_trace(cache_trace_path)

rate = 0.25
seed = 42

""" We can sample larger regions by ignoring lower order bits. 0 means no bits ignored. So
we sample individual cache blocks. Ignoring 1 lower bits means sample in groups (0,1),
(2,3) and (4,5). Ignoring 2 lower order bits means sample 
in groups (0,1,2,3), (4,5,6,7) and so on. """
num_lower_addr_bits_ignored = 0 

# generate a file with hash value for each addresses in the sample 
hash_file_path = "hash.csv"
cache_trace_reader = CacheTraceReader(cache_trace_path)
cache_trace_reader.create_sample_hash_file(seed, num_lower_addr_bits_ignored, hash_file_path)

# the hash file can now be used to generate samples 
sample_file_path = "sample.csv"
cache_trace_reader.sample_using_hash_file(hash_file_path, 
                                          rate, 
                                          num_lower_addr_bits_ignored, 
                                          sample_file_path)
```


# Usage

## Install 
```
git clone https://github.com/pbhandar2/phdthesis
cd phdthesis/cydonia
pip3 install . --user
```


## License
See [LICENSE](LICENSE) for details.

---



# References
```
1. @inproceedings{waldspurger15-efficient,
  author = {Carl A. Waldspurger and Nohhyun Park and Alexander Garthwaite and Irfan Ahmad},
  title = {Efficient MRC Construction with SHARDS},
  booktitle = {Proceedings of the 13th USENIX Conference on File and Storage Technologies},
  year = {2015},
  url = {https://www.usenix.org/system/files/conference/fast15/fast15-paper-waldspurger.pdf},
}
```
