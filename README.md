# Cydonia

Cydonia is a python package for analyzing workloads and experiment outputs 
from block storage trace replay using the block storage system stressor implemented
in our fork of [CacheLib](https://github.com/pbhandar2/CacheLib).

## Sampling

Cydonia uses randomized spatial sampling used in the FAST 15 paper ["Efficient MRC Construction with SHARDS"](https://www.usenix.org/system/files/conference/fast15/fast15-paper-waldspurger.pdf) to sample block addresses. Miss ratio curves (MRCs) generated from block addresses sampled using randomized spatial sampling have shown to have an average miss ratio error of less than 0.01 when using sampling rates lower than 1%[1]. This sampling approach uses fixed sized blocks. However, we can break a multi-block storage request into individual blocks during sampling. Cydonia augments the spatial sampling with different functions to generate the timestamps of sampled requests to generate sample block traces that not only contain the sampled block addresses but resemble the original block trace in format. 


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