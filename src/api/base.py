import json

from src.utils.logger import BaseModuleWithLogging


class APIBaseConnector(BaseModuleWithLogging):

    def __init__(self, name: str, credentials_file_path: str):

        super().__init__(name)
        self.credentials_file_path = credentials_file_path

        # load API credentials
        f = open(credentials_file_path, 'r')
        api_credentials = json.load(f)
        self.api_key = api_credentials['api-key']
        self.api_domain = api_credentials['api-domain']
        f.close()
