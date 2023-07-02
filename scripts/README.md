# Scripts

This directory contains python scirpts that utilize the cyodnia package. 

### sample/Sample.py
```
usage: Sample.py [-h] [--ts {iat,ts,iat2}] [--rate [RATE [RATE ...]]] [--bits [BITS [BITS ...]]] [--seed SEED] block_trace_path output_dir

Generate sample block traces from a block trace

positional arguments:
  block_trace_path      Path to the block trace to be sampled
  output_dir            Directory to store the sample block traces generated

optional arguments:
  -h, --help            show this help message and exit
  --ts {iat,ts,iat2}    Method to generate timestamps (Default: 'ts')
  --rate [RATE [RATE ...]]
                        The list of sample rates to evaluate
  --bits [BITS [BITS ...]]
                        List of different number of lower order bits to ignore
  --seed SEED           Random seed

Notes:
* Example usage: python3 Sample.py ~/w20.csv /dev/shm --rate 0.01 0.5 --bits 0 1 2
* The output sample file name has format: $WORKLOAD_FILE_NAME$_$SAMPLE_RATE$_$SEED$_$BITS$.csv
* For example: original trace w20.csv can generate samples with file name w20_10_42_2.csv
```

### sample/Profiler.py
```
usage: Profiler.py [-h] original_trace_path sample_trace_path output_path

Profile sample and its corresponding full block straoge trace

positional arguments:
  original_trace_path  Path to a full trace or a directory containing full traces
  sample_trace_path    Path to a sample or a directory containing samples
  output_path          Output path of file with workload feature or directory to output such files

optional arguments:
  -h, --help           show this help message and exit

Notes:
* Example usage: python3 Profiler.py ~/original_trace.csv ~/samples/ ~/sample_data.csv
* Sample file name should have the format: $WORKLOAD_NAME$_$REPLAY_RATE$_$SEED$_$BITS$.csv
* The corresponding full trace file name of each sample has the format: $WORKLOAD_NAME$.csv
* This workload name of sample and original trace is matched to ensured correct files are used
* The output file name has the format: $WORKLOAD_NAME$.csv
```