from datetime import datetime, timezone
from dateutil import parser

from src.utils.mindex import MultiIndex
from src.storage.s3 import S3StorageConnector
from src.api.sec import SECAPIConnector
from src.data.base import BaseDataLoaderModule


class SEC4DataLoader(BaseDataLoaderModule):

    def __init__(self, s3_connector: S3StorageConnector, sec_connector: SECAPIConnector, 
                 tickers: MultiIndex, manifest_s3_bucket_name: str, 
                 manifest_s3_object_name: str, data_s3_bucket_name: str, 
                 delay_time_secs: int=0,
                 fetch_from_override_dt: datetime=None):

        super().__init__(self.__class__.__name__, s3_connector, manifest_s3_bucket_name,
                         manifest_s3_object_name, data_s3_bucket_name)

        self.sec_connector = sec_connector
        self.tickers = tickers
        self.delay_time_secs = delay_time_secs
        self.fetch_from_override_dt = fetch_from_override_dt

    def update(self) -> bool:
        try:
            self.logger.info('Starting update routine.')

            # get update manifest
            self.logger.info('Loading manifest.')
            manifest = self._load_manifest()
            if manifest is not None:
                fetch_from_dt = parser.parse(manifest['fetch_from_dt'])
                fetch_from_dt = fetch_from_dt.astimezone(timezone.utc)
            elif self.fetch_from_override_dt is not None:
                fetch_from_dt = self.fetch_from_override_dt
                fetch_from_dt = fetch_from_dt.astimezone(timezone.utc)
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
                query_result = self.sec_connector.query_form_4_filings(
                    fetch_from_dt=fetch_from_dt,
                    tickers=self.tickers
                )

                # verify result
                if query_result is None:
                    self.logger.error('Filings query failed.')
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

                # save form 4 data
                self.logger.info('Saving new filings data.')
                for filing in filings:
                    id = filing['id']
                    ticker = filing['issuer']['ticker']
                    date = filing['report_period']
                    data_path = '{}/{}.json'.format(ticker, date)
                    save_result = self._save_data(data_path, filing)
                    if not save_result:
                        self.logger.error('Failed to save filing {}.'.format(id))
                        return False

                query_iteration += 1
                total_filings += len(filings)

            self.logger.info('Finishing update routine.')
            return True
        
        except Exception as e:
            self.logger.exception('Error in update: ' + str(e))
            return False