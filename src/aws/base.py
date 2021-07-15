import json

from src.utils.logger import BaseModuleWithLogging


class BaseAWSConnector(BaseModuleWithLogging):

    def __init__(self, name: str, credentials_file_path: str):

        super().__init__(name)
        self.credentials_file_path = credentials_file_path

        # load AWS credentials
        f = open(credentials_file_path, 'r')
        aws_credentials = json.load(f)
        self.access_key_id = aws_credentials['access_key_id']
        self.secret_access_key = aws_credentials['secret_access_key']
        self.region_name = aws_credentials['region_name']
        f.close()

