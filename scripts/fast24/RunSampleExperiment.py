from pathlib import Path 
from argparse import ArgumentParser


class RunSampleExperiment:
    def __init__(self) -> None:
        self.sample_dir = Path("/research2/mtc/cp_traces/pranav/sample/iat")
        self.sample_dir.mkdir(exist_ok=True, parents=True)

        self.sample_metadata_dir = Path("/research2/mtc/cp_traces/pranav/meta/sample/iat")
        self.sample_metadata_dir.mkdir(exist_ok=True, parents=True)

    
    def run(self) -> None:
        pass 


if __name__ == "__main__":
    runner = RunSampleExperiment()
    runner.run()
