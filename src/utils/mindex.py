import re
from typing import Any
import pickle
import uuid


class MultiIndex(object):

    def __init__(self, index_keys: list,
                 default_index_key: str=None,
                 safe_mode: bool=False):

        self.index_keys = index_keys
        self.default_index_key = default_index_key
        self.safe_mode = safe_mode
        if len(self.index_keys) == 0:
            raise Exception('must specify at least on index key')
        if self.default_index_key is not None and self.default_index_key not in self.index_keys:
            raise Exception('default index key must be in index keys')
        if self.safe_mode and default_index_key is None:
            raise Exception('default index key must be specified in safe mode')
            
        # define index tables
        self.index_tables = {}
        for index_key in self.index_keys:
            self.index_tables[index_key] = set()

        # define lookup tables
        self.lookup_tables = {}
        for index_key in self.index_keys:
            self.lookup_tables[index_key] = dict()

        # define hash table
        self.hash_table = {}
        self.iteration = 0

    def __len__(self) -> int:
        return len(self.hash_table)
    
    def __iter__(self) -> 'MultiIndex':
        return self

    def __next__(self) -> dict:
        objs = self.get_all()
        if self.iteration >= len(objs):
            self.reset()
            raise StopIteration
        else:
            obj = objs[self.iteration]
            self.iteration += 1
            return obj
            
    def reset(self) -> None:
        self.iteration = 0
    
    def is_finished(self) -> None:
        objs = self.get_all()
        return self.iteration >= len(objs)

    @staticmethod
    def load(path: str) -> Any:
        f = open(path, 'rb')
        multi_index = pickle.load(f)
        f.close()
        return multi_index

    def save(self, path: str) -> 'MultiIndex':
        f = open(path, 'wb+')
        pickle.dump(self, f)
        f.close()

    def insert(self, obj: dict):
        index_keys_copy = self.index_keys.copy()

        # verify object integrity
        for k, v in obj.items():
            if k in index_keys_copy: 
                index_keys_copy.remove(k)
                if v in self.index_tables[k]:
                    raise Exception('collision on index key: \"{}:{}\"'.format(k, v))
        if not self.safe_mode and len(index_keys_copy) > 0:
            raise Exception('not all index keys specified')
        elif self.safe_mode and self.default_index_key in index_keys_copy:
            raise Exception('default index key must be specified in safe mode')

        # insert object indices
        hash_key = str(uuid.uuid4())
        for k, v in obj.items():
            if k in self.index_keys:
                self.index_tables[k].add(v)
                self.lookup_tables[k][v] = hash_key

        # insert object
        self.hash_table[hash_key] = obj
    
    def get(self, key: str, value: Any) -> dict:
        if value is None:
            return None
        elif key not in self.index_keys:
            raise Exception('invalid index key; {}'.format(key))
        elif value not in self.index_tables[key]:
            if self.safe_mode: return None
            else: raise Exception('index key value not found: \"{}:{}\"'.format(key, value))
        else:
            hash_key = self.lookup_tables[key][value]
            return self.hash_table[hash_key]

    def remove(self, key: str, value: Any) -> None:

        # get object
        obj = self.get(key, value)
        if obj is None:
            raise Exception('index key value not found: \"{}:{}\"'.format(key, value))

        # remove object indices
        hash_key = None
        for k in self.index_keys:
            if k in obj:
                hash_key = self.lookup_tables[k][obj[k]]
                self.index_tables[k].remove(obj[k])
                self.lookup_tables[k].pop(obj[k])
            
        # remove object
        if hash_key is None:
            raise Exception('failed to locate hash key')
        else:
            self.hash_table.pop(hash_key)

    def get_all(self) -> list:
        return list(self.hash_table.values())

    def get_indices(self) -> list:
        return self.index_keys

    def get_all_key_values(self, key: str) -> list:
        if key in self.index_keys:
            values = list(self.hash_table.values())
            key_values = [obj[key] for obj in values]
            return key_values
        else:
            raise Exception('invalid index key')