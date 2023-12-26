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
A multi-block request could fragment into multiple samples block requests. We tackle this using an idea by Carl Waldspurger to ignore some bits in the addresses to sample group of blocks rather than individual blocks. Below is an example:

```
from cydonia.sample.Sampler import Sampler

block_trace_path = "/home/cydonia/block_trace.csv"
rate = 0.25
seed = 42
sampler = Sampler(block_trace_path)

"""
- "sample_df" is a pandas DataFrame of the sample trace. It has the same format as the 
original trace. 

- "sampling_split_percentage" is the percentage of block requests that were sampled 
that broke into multiple block requests in the sample. It can be used as a measure 
of sample quality. 

- For example, a multi-block request reads blocks 0-20, but only fraction of the blocks
in the block request were sampled: (2,3,4), (8), (11,12). Now the 3 groups of block 
requests that were sampled will be 3 different block requests in the sample although 
it originated from a single block request in the original trace. 
"""

sample_df, sampling_split_percentage = sampler.sample(rate, seed)

"""
- Now we  remap addresses to ignore bits at index 0, 1 and 2. This way we 
sample a group of addresses rather than individual addresses which should
reduce fragmentation measued by "sampling_split_percentage ".

- For example, we ignore the bits at index 0,1 and 2. Now address 8 (1000),
9 (1001), 10 (1010) all map to 8. If 8 is sampled, this means 9 and 10 
will be sampled as well.
"""

bits = [0, 1, 2]
bits_sampler = Sampler(block_trace_path, bits=bits)
bits_sample_df, bits_sampling_split_percentage = sampler.sample(rate, seed)

# by ignoring few bits and sampling multiple adjacent blocks, we should be able to reduce
# the bit_sampling_split_percentage and improve block sample quality 
assert (bit_sampling_split_percentage <= sampling_split_percentage)

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
