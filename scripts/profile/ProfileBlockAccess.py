
import sys 
from cydonia.profiler.BlockAccessTraceProfiler import BlockAccessTraceProfiler

workload = sys.argv[1]
profiler = BlockAccessTraceProfiler("/research2/mtc/cp_traces/pranav/block_access_traces/{}.csv".format(workload))
profiler.profile(output_path="/research2/mtc/cp_traces/pranav/block_access_window_stat/{}.csv".format(workload))

