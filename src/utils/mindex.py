from typing import Any
import pickle
import uuid


class MultiIndex(object):

    def __init__(self, index_keys: list):
        self.index_keys = index_keys
        assert(len(self.index_keys) > 0)

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
                    raise Exception('collision on index key {}'.format(k))
        if len(index_keys_copy) > 0:
            raise Exception('not all index keys specified')

        # insert object indices
        hash_key = str(uuid.uuid4())
        for k, v in obj.items():
            if k in self.index_keys:
                self.index_tables[k].add(v)
                self.lookup_tables[k][v] = hash_key

        # insert object
        self.hash_table[hash_key] = obj

    def get(self, key: str, value: Any) -> dict:
        if key not in self.index_keys:
            raise Exception('invalid index key')
        elif value not in self.index_tables[key]:
            raise Exception('index key value not found')
        else:
            hash_key = self.lookup_tables[key][value]
            return self.hash_table[hash_key]

    def remove(self, key: str, value: Any) -> None:

        # remove object indices
        obj = self.get(key, value)
        for k in self.index_keys:
            v = obj[k]
            hash_key = self.lookup_tables[k][v]
            self.index_tables[k].remove(v)
            self.lookup_tables[k].pop(v)
        
        # remove object
        self.hash_table.pop(hash_key)

    def get_all(self) -> list:
        return list(self.hash_table.values())

    def get_indices(self) -> list:
        return self.index_keys