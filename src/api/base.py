from typing import Any
from pathlib import Path
import atexit
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
        self.api_key = api_credentials['api-key']
        self.api_domain = api_credentials['api-domain']
        f.close()

        # initialize cache
        self.cache_file = 'cache/' + name + '.json'
        Path('cache').mkdir(parents=True, exist_ok=True)
        if self.load_cache and Path(self.cache_file).exists():
            self.logger.info('Loading cache.')
            self.api_cache = json.load(open(self.cache_file, 'r'))
        else:
            self.api_cache = {}
        atexit.register(self._dump_cache)

    def _add_cache(self, cache_id: str, cache_entry_id: str, cache_value: Any) -> None:
        if cache_id in self.api_cache:
            self.api_cache[cache_id][cache_entry_id] = cache_value
        else:
            self.api_cache[cache_id] = {}

    def _get_cache(self, cache_id: str, cache_entry_id: str) -> Any:
        if cache_id in self.api_cache:
            if cache_entry_id in self.api_cache[cache_id]: 
                return self.api_cache[cache_id][cache_entry_id]
            else:
                return None
        else:
            return None

    def _dump_cache(self) -> None:
        self.logger.info('Dumping API cache.')
        f = open(self.cache_file, 'w+')
        json.dump(self.api_cache, f)
        f.close()