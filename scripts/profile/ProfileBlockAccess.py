
import sys 
from cydonia.profiler.BlockAccessTraceProfiler import BlockAccessTraceProfiler

profiler = BlockAccessTraceProfiler(sys.argv[1])
profiler.profile(output_path=sys.argv[2])

