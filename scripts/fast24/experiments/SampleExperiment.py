""" This class generates block trace replay experiments to run based on the features of the workloads
    and samples provided by the user. """

import argparse 
import pathlib 
import pandas as pd 


class SampleExperiment:
    def __init__(self, feature_file_path):
        self.features = pd.read_csv(feature_file_path)
    
    
    def generate_experiment_file(self):
        pass 