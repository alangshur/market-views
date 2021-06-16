import logging
import sys

from src.utils.base import BaseModule


class LoggingModule(BaseModule):

    def __init__(self, name: str):
        super().__init__(self.__class__.__name__)

        # initialize python logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # build logging handler
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
        self.logger.addHandler(handler)

    def get_logger(self) -> logging.Logger:
        return self.logger


class BaseModuleWithLogging(BaseModule):

    def __init__(self, name: str):
        super().__init__(name)
        self.logger = LoggingModule(name).get_logger()
        if self.verbose: self.logger.info('Initializing module.')