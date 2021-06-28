from src.utils.logger import BaseModuleWithLogging


class BaseScraperModule(BaseModuleWithLogging):

    def __init__(self, name: str):
        super().__init__(name)
