from datetime import datetime, timezone
from dateutil import parser
import time

from src.aws.s3 import AWSS3Connector
from src.api.polygon import PolygonAPIConnector
from src.api.sec import SECAPIConnector
from src.raw.base import BaseLoaderModule


class SEC13FLoader(BaseLoaderModule):
    
    def __init__(self, s3_connector: AWSS3Connector, polygon_connector: PolygonAPIConnector, 
                 sec_connector: SECAPIConnector, manifest_s3_bucket_name: str,
                 manifest_s3_object_name: str, data_s3_bucket_name: str, 
                 delay_time_secs: int=0,
                 fetch_from_override_dt: datetime=None):

        super().__init__(self.__class__.__name__, s3_connector, manifest_s3_bucket_name,
                         manifest_s3_object_name, data_s3_bucket_name)

        self.polygon_connector = polygon_connector
        self.sec_connector = sec_connector
        self.delay_time_secs = delay_time_secs
        self.fetch_from_override_dt = fetch_from_override_dt

        # initialize monitor metrics
        self._add_monitor_metric('update_time_secs')
        self._add_monitor_metric('query_iterations')
        self._add_monitor_metric('total_filings')

    def update(self) -> bool:
        self.logger.info('Starting update routine.')
        self._refresh_monitor_metrics()
        start_time = time.time()

        # get update manifest
        self.logger.info('Loading manifest.')
        manifest = self._load_manifest()
        if manifest is not None:
            fetch_from_dt = parser.parse(manifest['fetch_from_dt'])
            fetch_from_dt = fetch_from_dt.astimezone(timezone.utc)
        elif self.fetch_from_override_dt is not None:
            fetch_from_dt = self.fetch_from_override_dt
        else:
            self.logger.error('Failed to load manifest.')
            return False

        # iteratively load filings
        query_iteration = 0
        total_filings = 0
        self.logger.info('Loading filings.')
        while True:
            self.logger.info('Loading filings: {}.'.format(fetch_from_dt.date()))

            # query SEC API
            query_result = self.sec_connector.query_13f_filings(
                fetch_from_dt=fetch_from_dt,
                polygon_connector=self.polygon_connector
            )

            # verify result
            if query_result is None:
                self.logger.info('Filings query failed.')
                return False
            elif len(query_result[0]) == 0:
                break
            else:
                filings, fetch_from_dt = query_result
        
            # save manifest
            self.logger.info('Saving new manifest.')
            save_result = self._save_manifest({
                'fetch_from_dt': fetch_from_dt.isoformat()
            })
            if not save_result:
                self.logger.error('Failed to save manifest.')
                return False

            # save 13f data
            self.logger.info('Saving new filings data.')
            for filing in filings:
                id = filing['id']
                cik = filing['cik']
                date = filing['report_period']
                save_result = self._save_data(cik, date, filing)
                if not save_result:
                    self.logger.error('Failed to save filing {}.'.format(id))
                    return False

            query_iteration += 1
            total_filings += len(filings)

        # update global metrics
        stop_time = time.time()
        self._replace_monitor_metric('update_time_secs', stop_time - start_time)
        self._replace_monitor_metric('query_iterations', query_iteration)
        self._replace_monitor_metric('filings_added', total_filings)
        self.logger.info('Stopping update routine.')
        return True