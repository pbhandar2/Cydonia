from socket import gethostname
from pathlib import Path 
from argparse import ArgumentParser
from pyJoules.device.rapl_device import RaplPackageDomain, RaplDramDomain
from pyJoules.energy_meter import EnergyMeter


class RunBlockTraceReplay:
    def __init__(
        self, 
        machine_name: str
    ) -> None:
        self.machine_name = machine_name

        self.backing_file_path = Path.home().joinpath("disk/disk.file")
        self.nvm_file_path = Path.home().joinpath("nvm/disk.file")
        self.cachebench_binary_path = Path.home().joinpath("disk/CacheLib/opt/cachelib/bin/cachebench")
        self.power_meter = EnergyMeter(DeviceFactory.create_devices([RaplPackageDomain(0), RaplDramDomain(0)]))
        self.hostname = gethostname()

        self.output_dir = Path("/dev/shm/block_replay")
        self.output_dir.mkdir(exist_ok=True)

        # output files from each block trace replay 
        self.power_usage_file_path = self.output_dir.joinpath("power.csv")
        self.cpu_memory_usage_file_path = self.output_dir.joinpath("cpu_mem.csv")
        self.stdout_file_path = self.output_dir.joinpath("stdout.dump")
        self.stat_file_path = self.output_dir.joinpath("stat_0.out")
        self.tsstat_file_path = self.output_dir.joinpath("tsstat_0.out")
    

    def run(self) -> None:
        pass 


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Run block trace replay.")
    
    parser.add_argument(
        "config_file_path",
        type=Path,
        help="Path to configuration file to run block trace replay.")
    
    args = parser.parse_args()