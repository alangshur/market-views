from abc import abstractmethod

from src.aws.s3 import AWSS3Connector
from src.utils.logger import BaseModuleWithLogging


class BaseLoaderModule(BaseModuleWithLogging):

    def __init__(self, name: str, s3_connector: AWSS3Connector, 
                 manifest_s3_bucket_name: str, manifest_s3_object_name: str,
                 data_s3_bucket_name: str):

        super().__init__(name)
        self.s3_connector = s3_connector
        self.manifest_s3_bucket_name = manifest_s3_bucket_name
        self.manifest_s3_object_name = manifest_s3_object_name
        self.data_s3_bucket_name = data_s3_bucket_name
        self.monitor_metrics = {}

    @abstractmethod
    def update(self) -> bool:
        raise NotImplemented

    def get_monitor_metrics(self) -> dict:
        return self.monitor_metrics

    def _load_manifest(self) -> dict:
        return self.s3_connector.read_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name)
        
    def _save_manifest(self, manifest: dict) -> bool:
        return self.s3_connector.write_json(self.manifest_s3_bucket_name, self.manifest_s3_object_name, manifest)

    def _save_data(self, object_class: str, object_date: str, data: dict) -> bool:
        object_name = '{}/{}.json'.format(object_class, object_date)
        return self.s3_connector.write_json(self.data_s3_bucket_name, object_name, data)

    def _add_monitor_metric(self, metric_id: str) -> None:
        self.monitor_metrics[metric_id] = []

    def _refresh_monitor_metrics(self) -> None:
        for metric_id in self.monitor_metrics:
            self.monitor_metrics[metric_id].append(0.0)
        
    def _increment_monitor_metric(self, metric_id: str) -> None:
        if metric_id in self.monitor_metrics:
            self.monitor_metrics[metric_id][-1] += 1.0

    def _update_monitor_metric(self, metric_id: str, metric_value: float) -> None:
        if metric_id in self.monitor_metrics:
            self.monitor_metrics[metric_id][-1] += float(metric_value)

    def _replace_monitor_metric(self, metric_id: str, metric_value: float) -> None:
        if metric_id in self.monitor_metrics:
            self.monitor_metrics[metric_id][-1] = float(metric_value)
