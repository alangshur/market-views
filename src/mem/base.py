from typing import Any
from abc import abstractmethod

from src.utils.logger import BaseModuleWithLogging
from src.storage.redis import RedisStorageConnector


class BaseMemLoaderModule(BaseModuleWithLogging):

    def __init__(self, name: str, redis_connector: RedisStorageConnector):
        super().__init__(name)
        self.redis_connector = redis_connector

    @abstractmethod
    def update(self) -> bool:
        raise NotImplemented

    def _save_data(self, data_name: str, data: Any) -> bool:
        return self.redis_connector.set(data_name, data)