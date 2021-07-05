from abc import abstractmethod
import json
import os

from src.aws.s3 import AWSS3Connector
from src.utils.logger import BaseModuleWithLogging


class BaseManagerModule(BaseModuleWithLogging):

    def __init__(self, name: str, aws_s3_connector: AWSS3Connector, 
                 manifest_s3_bucket_name: str, manifest_s3_object_name: str):

        super().__init__(name)
        self.aws_s3_connector = aws_s3_connector
        self.manifest_s3_bucket_name = manifest_s3_bucket_name
        self.manifest_s3_object_name = manifest_s3_object_name

    @abstractmethod
    def update(self) -> None:
        raise NotImplemented

    def _load_manifest(self) -> dict:
        return self.aws_s3_connector.read_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name)
        
    def _save_manifest(self, manifest: dict) -> bool:
        return self.aws_s3_connector.write_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name, manifest)