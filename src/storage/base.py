import json

from src.utils.logger import BaseModuleWithLogging


class BaseStorageConnector(BaseModuleWithLogging):

    def __init__(self, name: str, credentials_file_path: str):
        super().__init__(name)
        self.credentials_file_path = credentials_file_path

        # load credentials
        f = open(credentials_file_path, 'r')
        self.storage_credentials = json.load(f)
        f.close()

