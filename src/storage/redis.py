from typing import Any
import cachelib

from src.storage.base import BaseStorageConnector


class RedisStorageConnector(BaseStorageConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

        # get redis credentials
        host = self.storage_credentials['host']
        port = self.storage_credentials['port']

        # connect to redis
        self.redis = cachelib.RedisCache(
            host=host,
            port=port,
            db=0,
            default_timeout=0,
            socket_keepalive=True
        )

        # init key store
        self.key_store = {}

    def get(self, key) -> Any:
        try:
            obj = self.redis.get(key)
            return obj
        except Exception as e:
            self.logger.exception('Exception in get: {}.'.format(e))
            return None

    def set(self, key, value, timeout=None) -> bool:
        try:
            self.redis.set(key, value, timeout=timeout)
            self.key_store.add(key)
            return True
        except Exception as e:
            self.logger.exception('Exception in set: {}.'.format(e))
            return False

    def delete(self, key) -> bool:
        try:
            self.redis.delete(key)
            self.key_store.discard(key)
            return True
        except Exception as e:
            self.logger.exception('Exception in delete: {}.'.format(e))
            return False