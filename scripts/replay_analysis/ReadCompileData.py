import pandas as pd 


class CompileDataReader:
    def __init__(self):
        df = pd.read_csv("./data/overall.csv")
        #print("Here")
        #print(df[['workload', 'machine', 'rate', 'seed', 'bits', 'it', 't1', 't2', 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns']])

        for group_index, group_df in df.groupby(['workload', 't1', 't2']):
            print(group_df[['workload', 
                                'machine', 
                                'rate', 
                                'seed', 
                                'bits', 
                                'it', 
                                't1', 
                                't2', 
                                'ramHitRatio_%',
                                'nvmHitRatio_%',
                                'blockReadLatency_avg_ns', 
                                'blockWriteLatency_avg_ns']])
    

reader = CompileDataReader()

