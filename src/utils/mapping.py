import requests

from src.utils.logger import BaseModuleWithLogging
from src.api.polygon import PolygonAPIConnector


class MappingModule(BaseModuleWithLogging):

    def __init__(self, polygon_connector: PolygonAPIConnector):
        super().__init__(self.__class__.__name__)

        self.polygon_connector = polygon_connector

    