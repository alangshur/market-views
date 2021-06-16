from abc import ABC
from src.utils.logger import LoggingModule


class UtilityModule(ABC):

    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name


class LoggingUtilityModule(UtilityModule):

    def __init__(self, name):
        super().__init__(name)
        self.logger = LoggingModule(name)