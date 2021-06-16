from abc import ABC


class BaseModule(ABC):

    def __init__(self, name: str, 
                 verbose: bool=True):
                 
        self.name = name
        self.verbose = verbose

    def get_name(self) -> str:
        return self.name