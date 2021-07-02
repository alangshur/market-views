from abc import abstractmethod
import json
import os

from src.aws.s3 import AWSS3Connector
from src.utils.logger import BaseModuleWithLogging


class BaseManagerModule(BaseModuleWithLogging):

    def __init__(self, name: str, aws_s3_connector: AWSS3Connector):
        super().__init__(name)
        self.aws_s3_connector = aws_s3_connector

    @abstractmethod
    def update(self) -> None:
        raise NotImplemented

    def _load_manifest(self) -> dict:
        return self.aws_s3_connector.get_raw_manifest(self.get_name())

    def _save_manifest(self, manifest: dict) -> None:
        self.aws_s3_connector.save_raw_manifest(self.get_name(), manifest)