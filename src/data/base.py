from abc import abstractmethod

from src.storage.s3 import S3StorageConnector
from src.utils.logger import BaseModuleWithLogging


class BaseDataLoaderModule(BaseModuleWithLogging):

    def __init__(self, name: str, s3_connector: S3StorageConnector, 
                 manifest_s3_bucket_name: str, manifest_s3_object_name: str,
                 data_s3_bucket_name: str):

        super().__init__(name)
        self.s3_connector = s3_connector
        self.manifest_s3_bucket_name = manifest_s3_bucket_name
        self.manifest_s3_object_name = manifest_s3_object_name
        self.data_s3_bucket_name = data_s3_bucket_name

    @abstractmethod
    def update(self) -> bool:
        raise NotImplemented

    def _load_manifest(self) -> dict:
        return self.s3_connector.read_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name)
        
    def _save_manifest(self, manifest: dict) -> bool:
        return self.s3_connector.write_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name, manifest)

    def _save_data(self, data_name: str, data: dict) -> bool:
        return self.s3_connector.write_json(self.data_s3_bucket_name, data_name, data)