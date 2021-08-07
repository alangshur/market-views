from abc import abstractmethod

from src.utils.logger import BaseModuleWithLogging


class BaseMemLoaderModule(BaseModuleWithLogging):

    def __init__(self, name: str):
        super().__init__(name)
        self.monitor_metrics = {}

        # TODO: store in memcache or redis

    @abstractmethod
    def update(self) -> bool:
        raise NotImplemented

    def get_monitor_metrics(self) -> dict:
        return self.monitor_metrics

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
