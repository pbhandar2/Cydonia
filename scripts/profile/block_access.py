from pathlib import Path 
from subprocess import Popen, PIPE 
from argparse import ArgumentParser
from pandas import read_csv

import logging 
from logging import getLogger, Formatter
from logging.handlers import RotatingFileHandler


DEFAULT_LBA_SIZE_BYTE = 512
DEFAULT_BLOCK_SIZE_BYTE = 4096


class GenerateBlockAccess:
    """GenerateBlockAccess generates block access trace from a block storage trace. Each row of a block storage 
    trace corresponding to a multi-block request. Each row of a block access trace represents an access to a fixed 
    sized block. 

    Attributes:
        _stack_binary_path: Path to the binary that generates stack distance given a stream of block accesses 
        _block_trace_path: Path to the block storage trace from which we generate block accesses to pass to stack binary 
        _lba_size_byte: Size of a logical block address (LBA) in block storage trace 
        _block_size_byte: Size of fixed-size blocks in cache 
        _logger_name: Name of the logger 
        _log_file_path: Path to log file 
        _logger: Logger that writes to log file 
    """
    def __init__(
        self, 
        block_trace_path: str,
        block_access_trace_path: str, 
        stack_distance_binary_path: str,
        lba_size_byte: int = DEFAULT_LBA_SIZE_BYTE, 
        block_size_byte: int = DEFAULT_BLOCK_SIZE_BYTE 
    ) -> None:
        self._stack_binary_path = stack_distance_binary_path
        self._block_trace_path = block_trace_path
        self._block_access_trace_path = block_access_trace_path
        self._lba_size_byte = lba_size_byte
        self._block_size_byte = block_size_byte
        self._logger_name = 'block_access_logger'
        self._log_file_path = Path('/dev/shm/block_access.log')
        self.logger = getLogger(self._logger_name)
        self._set_logging()
    

    def _set_logging(self) -> None:
        """Setup the log files for this script. """
        self.logger.setLevel(logging.INFO)
        logHandler = RotatingFileHandler(str(self._log_file_path.absolute()), maxBytes=25*1e6)
        logHandler.setLevel(logging.INFO)
        logHandler.setFormatter(Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s'))
        self.logger.addHandler(logHandler)


    def run(self) -> None:
        """Generate block access trace. """
        block_access_list = []
        block_df = read_csv(self._block_trace_path, names=["ts", "lba", "op", "size"])
        block_access_count = 0 
        for row_index, row in block_df.iterrows():
            lba = row['lba']
            start_block = int(row['lba']*self._lba_size_byte/self._block_size_byte)
            size = int((row['size']-1)/self._block_size_byte)
            for cur_block in range(start_block, start_block+size+1):
                block_access_list.append(str(cur_block))

        process = Popen([self._stack_binary_path], stdin=PIPE, stdout=PIPE)
        stdout = process.communicate(input="\n".join(block_access_list).encode("utf-8"))[0]
        rd_array = stdout.decode("utf-8").split("\n")

        with open(self._block_access_trace_path, "w+") as block_access_file_handle:
            block_access_count = 0 
            for row_index, row in block_df.iterrows():
                lba = row['lba']
                start_block = int(row['lba']*self._lba_size_byte/self._block_size_byte)
                size = int((row['size']-1)/self._block_size_byte)
                for cur_block in range(start_block, start_block+size+1):
                    block_access_file_handle.write("{},{},{},{}\n".format(row['ts'], cur_block, row['op'], rd_array[block_access_count]))
                    block_access_count += 1 
            assert len(rd_array)-1 == block_access_count, "{} != {}".format(len(rd_array), block_access_count)


if __name__ == "__main__":
    parser = ArgumentParser(description="Generate block access trace with fixed sized block and reuse distance"
                                            "from block storage trace with multi-block requests.")
    parser.add_argument("block_trace_path",
                        type=Path,
                        help="Path to block storage trace.")
    parser.add_argument("block_access_trace_path",
                        type=Path,
                        help="Path to block access trace.")
    parser.add_argument("stack_distance_binary_path",
                        type=Path,
                        help="Path to the binary that generates reuse distance.")
    parser.add_argument("--lba_size_byte",
                        default=DEFAULT_LBA_SIZE_BYTE,
                        type=int,
                        help="The size of an address or LBA (Logical Block Address).")
    parser.add_argument("--block_size_byte",
                        default=DEFAULT_BLOCK_SIZE_BYTE,
                        type=int,
                        help="The size of a block in cache.")
    args = parser.parse_args()

    generator = GenerateBlockAccess(
                    args.block_trace_path,
                    args.block_access_trace_path,
                    args.stack_distance_binary_path,
                    lba_size_byte = args.lba_size_byte,
                    block_size_byte = args.block_size_byte)
    generator.run()