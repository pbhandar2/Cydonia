import pathlib 
import argparse 
import pandas as pd 


class ReplayOutput:
    def __init__(self, out_path):
        self.path = pathlib.Path(out_path)
        self.load()
    
    def load(self):
        self.data = {}
        with self.path.open('r') as f:
            line = f.readline()
            while line:
                split_line = line.rstrip().split("=")
                if len(split_line) == 2:
                    metric_name, metric_val = split_line[0], float(split_line[1])
                    self.data[metric_name] = metric_val
                line = f.readline()
    
    def get_data(self):
        return self.data 


class CompileReplayOutput:
    def __init__(self, out_dir, out_path):
        self.dir = out_dir 
        self.out_path = out_path
    
    def load(self):
        replay_output_list = []
        print("Here")
        for machine_dir in self.dir.iterdir():
            machine_name = machine_dir.name
            for workload_dir in machine_dir.iterdir():
                workload_name = workload_dir.name
                split_workload_name = workload_name.split("_")
                rate, seed, bits = 0, 0, 0 
                if len(split_workload_name) > 1:
                    workload_name = split_workload_name[0]
                    rate = split_workload_name[1]
                    seed = split_workload_name[2]
                    bits = split_workload_name[3]

                for replay_output_dir in workload_dir.iterdir():

                    replay_dir_name = replay_output_dir.name 
                    split_replay_dir_name = replay_dir_name.split("_")
                    replay_params = {}
                    for dir_name_substr in split_replay_dir_name:
                        split_line = dir_name_substr.split("=")
                        replay_params[split_line[0]] = split_line[1]

                    output = ReplayOutput(replay_output_dir.joinpath("stat_0.out"))
                    print("loaded {}".format(replay_output_dir))
                    data = output.get_data()
                    data['workload'] = workload_name
                    data['rate'] = rate
                    data['seed'] = seed 
                    data['bits'] = bits
                    data['machine'] = machine_name
                    data['t1'] = replay_params['t1']
                    data['t2'] = replay_params['t2']
                    data['it'] = replay_params['it']
                    replay_output_list.append(data)
        df = pd.DataFrame(replay_output_list)
        df.to_csv(self.out_path, index=False)


OUTPUT_DIR = pathlib.Path("/research2/mtc/cp_traces/replay/done")
COMPILE_DATA_OUTPUT_PATH = pathlib.Path("./data/overall.csv")

compiler = CompileReplayOutput(OUTPUT_DIR, COMPILE_DATA_OUTPUT_PATH)
compiler.load()