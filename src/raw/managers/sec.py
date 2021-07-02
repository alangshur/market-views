from dateutil.relativedelta import relativedelta
from datetime import datetime
import time

from src.aws.s3 import AWSS3Connector
from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.base import BaseManagerModule


class SEC13FManager(BaseManagerModule):
    
    def __init__(self, sec_13f_scraper: SEC13FScraper, aws_s3_connector: AWSS3Connector,
                 default_history_size: relativedelta=relativedelta(days=1),
                 default_delay_time: int=0):

        super().__init__(self.__class__.__name__, aws_s3_connector)

        self.sec_13f_scraper = sec_13f_scraper 
        self.default_history_size = default_history_size
        self.default_delay_time = default_delay_time

    def update(self) -> None:

        # get manifest
        manifest = self._load_manifest()
        if manifest is None:
            history_size = self.default_history_size
            delay_time = self.default_delay_time
        else:
            history_size = manifest['default_history_size']
            delay_time = manifest['default_delay_time']

        # get start/end targets
        fetch_from_dt = datetime.now() + relativedelta(days=1)
        fetch_until_dt = fetch_from_dt - history_size
        
        # extract filing IDs
        start_epoch_time = time.time()
        self.logger.info('Loading filing IDs.')
        get_result = self._get_filing_ids(fetch_from_dt, fetch_until_dt, delay_time)
        if get_result is None: return None
        filing_ids, query_dts = get_result
        start_query_dt = query_dts[0]

        # extract filing data
        self.logger.info('Loading filing data.')
        get_result = self._get_filing_data(filing_ids, delay_time)
        if get_result is None: return None
        filing_data = get_result
        end_epoch_time = time.time()

        # save manifest
        self._save_manifest({
            'default_history_size': history_size,
            'default_delay_time': delay_time,
            'first_dt': str(start_query_dt.date()),
            'last_run_time_secs': float(end_epoch_time - start_epoch_time)
        })

        return filing_data

    def _get_filing_ids(self, fetch_from_dt: datetime, fetch_until_dt: datetime,
                        delay_time: int) -> list:

        next_query_dt = fetch_from_dt
        all_filing_ids = []
        all_query_dts = []

        # iteratively fetch historical filing IDs
        while next_query_dt > fetch_until_dt:
            time.sleep(delay_time)

            fetch_result = self.sec_13f_scraper.fetch_filing_ids(next_query_dt)
            if fetch_result is None: return None
            else:
                filing_ids, query_dts, next_query_dt = fetch_result
                all_filing_ids.extend(filing_ids)
                all_query_dts.extend(query_dts)

        return all_filing_ids, all_query_dts

    def _get_filing_data(self, filing_ids: list, delay_time: int) -> dict:

        all_filing_data = {}

        # iteratively fetch historical filing data
        for filing_id in filing_ids:
            time.sleep(delay_time)

            fetch_result = self.sec_13f_scraper.fetch_filing_data(filing_id)
            if fetch_result is None: return None
            else:
                filing_data, holdings_data = fetch_result
                all_filing_data[filing_id] = {
                    'filing': filing_data,
                    'holdings': holdings_data
                }

        return all_filing_data