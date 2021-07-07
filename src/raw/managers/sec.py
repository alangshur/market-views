from dateutil.relativedelta import relativedelta
from datetime import datetime
from dateutil import parser
from typing import Tuple
import time

from src.aws.s3 import AWSS3Connector
from src.api.polygon import APIPolygonConnector
from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.base import BaseManagerModule


class SEC13FManager(BaseManagerModule):
    
    def __init__(self, sec_13f_scraper: SEC13FScraper, s3_connector: AWSS3Connector, 
                 polygon_connector: APIPolygonConnector, manifest_s3_bucket_name: str, 
                 manifest_s3_object_name: str, data_s3_bucket_name: str,
                 default_history_size_days: int=1, default_delay_time_secs: int=0):

        super().__init__(self.__class__.__name__, s3_connector, manifest_s3_bucket_name,
                         manifest_s3_object_name, data_s3_bucket_name)

        self.sec_13f_scraper = sec_13f_scraper 
        self.polygon_connector = polygon_connector
        self.default_history_size_days = default_history_size_days
        self.default_delay_time_secs = default_delay_time_secs

        # initialize monitor metrics
        self._add_monitor_metric('update_time_secs')
        self._add_monitor_metric('totat_processed_filings_count')
        self._add_monitor_metric('skipped_filing_count')

    def update(self) -> None:
        self.logger.info('Starting update routine.')
        self._refresh_monitor_metrics()

        # get local or s3 manifest
        manifest = self._load_manifest()
        if manifest is None:
            self.logger.info('Loading local manifest.')
            delay_time_secs = self.default_delay_time_secs
            fetch_from_dt = datetime.now() + relativedelta(days=1)
            fetch_until_dt = datetime.now() - relativedelta(days=self.default_history_size_days)
        else:
            self.logger.info('Loading S3 manifest.')
            delay_time_secs = manifest['delay_time_secs']
            fetch_from_dt = datetime.now() + relativedelta(days=1)
            fetch_until_dt = parser.parse(manifest['prev_start_dt'])
        
        # extract filing IDs
        start_epoch_time = time.time()
        self.logger.info('Loading filing IDs.')
        filing_ids = self._get_filing_ids(fetch_from_dt, fetch_until_dt, delay_time_secs)
        if filing_ids is None: return None
        else: 
            filing_ids, query_dts = filing_ids
            start_query_dt = query_dts[0]

        # extract filing data
        self.logger.info('Loading filing data.')
        filing_data = self._get_filing_data(filing_ids, delay_time_secs)
        if filing_data is None: return None
        end_epoch_time = time.time()
        update_time_secs = float(end_epoch_time - start_epoch_time)

        # save manifest
        self.logger.info('Saving new manifest.')
        save_result = self._save_manifest({
            'delay_time_secs': delay_time_secs,
            'prev_start_dt': str(start_query_dt.date())
        })
        if not save_result: 
            self.logger.error('Failed to save manifest.')
            return None

        # save 13f data
        self.logger.info('Saving new SEC 13F report data.')
        for filing_id in filing_data:
            cik = filing_data[filing_id]['filing']['cik']
            date = filing_data[filing_id]['filing']['report_period']
            save_result = self._save_data(cik, date, filing_data[filing_id])
            if not save_result:
                self.logger.error('Failed to save filing {}.'.format(filing_id))
                return None

        # update global metrics
        self._replace_monitor_metric('total_filings_count', len(filing_ids))
        self._replace_monitor_metric('totat_processed_filings_count', len(filing_data))
        self._replace_monitor_metric('update_time_secs', update_time_secs)
        self.logger.info('Stopping update routine.')

    def _get_filing_ids(self, fetch_from_dt: datetime, fetch_until_dt: datetime,
                        delay_time_secs: int) -> Tuple[list, list]:

        next_query_dt = fetch_from_dt
        all_filing_ids = []
        all_query_dts = []

        # iteratively fetch historical filing IDs
        while next_query_dt >= fetch_until_dt:
            time.sleep(delay_time_secs)

            fetch_result = self.sec_13f_scraper.fetch_filing_ids(next_query_dt)
            if fetch_result is None: return None
            else:
                filing_ids, query_dts, next_query_dt = fetch_result
                all_filing_ids.extend(filing_ids)
                all_query_dts.extend(query_dts)

        return all_filing_ids, all_query_dts

    def _get_filing_data(self, filing_ids: list, delay_time_secs: int) -> dict:

        all_filing_data = {}

        # iteratively fetch historical filing data
        for filing_id in filing_ids:
            time.sleep(delay_time_secs)

            fetch_result = self.sec_13f_scraper.fetch_filing_data(filing_id)
            if fetch_result is None: 
                self._increment_monitor_metric('skipped_filing_count')
                continue
            else:
                filing_data, holdings_data = fetch_result

                # process holdings data
                holdings_data = self._process_holdings_data(holdings_data)
                all_filing_data[filing_id] = {
                    'filing': {
                        **{'filing_id': filing_id},
                        **filing_data,
                    },
                    'holdings': holdings_data
                }

        return all_filing_data

    def _process_holdings_data(self, holdings: list) -> dict:

        # merge repeated holdings
        cusip_filt_holdings = {}
        for holding in holdings:
            if holding['cusip'] in cusip_filt_holdings:
                cusip_filt_holdings[holding['cusip']]['value'] += holding['value']
                cusip_filt_holdings[holding['cusip']]['shares'] += holding['shares']
            else:
                cusip_filt_holdings[holding['cusip']] = holding

        # map cusip to ticker with polygon
        ticker_filt_holdings = {}
        for cusip in cusip_filt_holdings:
            ticker = self.polygon_connector.query_ticker_with_cusip(cusip)
            if ticker is not None: 
                ticker_filt_holdings[ticker] = {
                    **{'ticker': ticker},
                    **cusip_filt_holdings[cusip]
                }

        return ticker_filt_holdings

    def _save_report(self):
        pass