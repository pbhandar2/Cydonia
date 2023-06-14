# Cydonia

Cydonia is a python package with tools for analyzing workloads and experiment outputs 
from block storage trace replay using the block storage system stressor implemented
in our fork of [CacheLib](https://github.com/pbhandar2/CacheLib).

## Sampling

Cydonia uses SHARDS (Spatially Hashed Approximate Reuse Distance
Sampling), presented in the FAST 15 paper ["Efficient MRC Construction with SHARDS"](https://www.usenix.org/system/files/conference/fast15/fast15-paper-waldspurger.pdf), to spatially sample block storage traces[1]. It has generated highly accurate miss ratio curves (MRCs) with low sampling rates. However, when replaying a block storage 
trace to evaluate system performance, we need both spatial and temporal components of the trace. 
One simple approach is to use the timestamps of the sampled block requests but this approach 
inflates the inter-arrival times (IATs) of block requests in the sample compared to the original. Cydonia uses the IATs of sampled block requests to generate timestamps. This approach generates samples with IATs that 
are more representative of the original compared to using timestamps of the sampled block requests. 


# Usage
### Install 
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