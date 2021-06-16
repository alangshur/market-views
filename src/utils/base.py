from abc import ABC


class BaseModule(ABC):

    def __init__(self, name, verbose=True):
        self.name = name
        self.verbose = verbose

    def get_name(self):
        return self.name