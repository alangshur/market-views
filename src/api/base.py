from datetime import datetime, timezone, timedelta
from time import time
from typing import Any
from pathlib import Path
import atexit
import pickle
import json

from src.utils.logger import BaseModuleWithLogging


class BaseAPIConnector(BaseModuleWithLogging):

    def __init__(self, name: str, credentials_file_path: str,
                 load_cache: bool=True):

        super().__init__(name)
        self.credentials_file_path = credentials_file_path
        self.load_cache = load_cache

        # load API credentials
        f = open(credentials_file_path, 'r')
        api_credentials = json.load(f)
        self.api_key = api_credentials.get('api-key')
        self.api_domain = api_credentials.get('api-domain')
        self.api_credentials = api_credentials
        f.close()

        # initialize cache
        self.cache_file = 'cache/' + name + '.pkl'
        Path('cache').mkdir(parents=True, exist_ok=True)
        if self.load_cache and Path(self.cache_file).exists():
            self.logger.info('Loading cache.')
            f = open(self.cache_file, 'rb')
            self.api_cache = pickle.load(f)
            f.close()
        else:
            self.api_cache = {}

        # register cache dump
        atexit.register(self._dump_cache)

    def _add_cache(self, cache_id: str, cache_entry_id: str, cache_value: Any,
                   expiry_delta: timedelta=None) -> None:

        # get cache expiry target
        if expiry_delta is not None:
            expiry_dt = datetime.now(tz=timezone.utc) + expiry_delta
        else:
            expiry_dt = None

        # insert cache entry
        if cache_id in self.api_cache:
            self.api_cache[cache_id][cache_entry_id] = (cache_value, expiry_dt)
        else:
            self.api_cache[cache_id] = {}
            self.api_cache[cache_id][cache_entry_id] = (cache_value, expiry_dt)

    def _get_cache(self, cache_id: str, cache_entry_id: str) -> Any:

        # retrieve cache entry
        if cache_id in self.api_cache:
            if cache_entry_id in self.api_cache[cache_id]: 
                cache_entry = self.api_cache[cache_id][cache_entry_id]
                if cache_entry is None: 
                    return None
                else:
                    cache_value, expiry_dt = cache_entry
                    if expiry_dt is None: 
                        return cache_value
                    else:
                        current_dt = datetime.now(tz=timezone.utc)
                        if current_dt >= expiry_dt:
                            self.api_cache[cache_id].pop(cache_entry_id)
                            return None
                        else:
                            return cache_value
            else:
                return None
        else:
            return None

    def _dump_cache(self) -> None:
        self.logger.info('Dumping API cache.')
        f = open(self.cache_file, 'wb+')
        pickle.dump(self.api_cache, f)
        f.close()