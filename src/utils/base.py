from abc import ABC


class BaseModule(ABC):

    def __init__(self, name: str):
        self.name = name

    def get_name(self) -> str:
        return self.name