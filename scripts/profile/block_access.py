
from pathlib import Path 
from argparse import ArgumentParser

import logging 
from logging import getLogger, Formatter
from logging.handlers import RotatingFileHandler

class GenerateBlockAccess:
    def __init__(
        self, 
        stack_distance_binary_path: str 
    ) -> None:
        # setup logging 
        self._stack_binary_path = stack_distance_binary_path
        self._log_file_path = Path('/dev/shm/block_access.log')
        self._logger_name = 'block_access_logger'
        self._set_logging()
    

    def _set_logging(self) -> None:
        """Setup the log files for this script. """
        self.logger = getLogger(self._logger_name)
        self.logger.setLevel(logging.INFO)
        logHandler = RotatingFileHandler(str(self._log_file_path.absolute()), maxBytes=25*1e6)
        logHandler.setLevel(logging.INFO)
        logHandler.setFormatter(Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s'))
        self.logger.addHandler(logHandler)


if __name__ == "__main__":
    pass 