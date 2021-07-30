from datetime import timedelta, date
from time import time
from typing import Any
import requests

from src.api.base import BaseAPIConnector
from src.utils.mindex import MultiIndex


class SECGovAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str,
                 cache_expiry_delta: timedelta=timedelta(days=1)):

        super().__init__(self.__class__.__name__, credentials_file_path)
        self.cache_expiry_delta = cache_expiry_delta

    def get_ciks(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            legal ticker types from sec.gov:

            - ticker (index)
            - cik (index)
            - name (index)
        """

        indices = [
            'ticker', 'cik', 'name'
        ]

        try:

            # check cache
            cached_item = self._get_cache('get_ciks', 'all')
            if cached_item is not None:
                return cached_item
            
            # get tickers data
            response = requests.get(self.api_domain + 'company_tickers.json')
            response.raise_for_status()          
            data = response.json()
            
            # build multi-index
            multi_index = MultiIndex(indices)
            for _, v in data.items():
                try:
                    multi_index.insert({
                        'cik': v['cik_str'],
                        'ticker': v['ticker'],
                        'name': v['title']
                    })
                except Exception:
                    continue
            
            # cache item
            self._add_cache('get_ciks', 'all', multi_index, 
                            expiry_delta=self.cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_ciks: ' + str(e))
            return None

    def get_cusips(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            legal ticker types from sec.gov:

            - ticker (index)
            - cik (index)
            - name (index)
        """

        indices = [
            'ticker', 'cik', 'name'
        ]

        try:

            # check cache
            cached_item = self._get_cache('get_cusips', 'all')
            if cached_item is not None:
                return cached_item

            # get date code
            cur_date = date.today() - timedelta(days=90)
            date_code = cur_date.strftime('%Y%ma')
            query_code = 'data/fails-deliver-data/cnsfails' + date_code

            # get tickers data
            response = requests.get(self.api_domain + query_code)
            response.raise_for_status()          
            data = response.json()
            
            # build multi-index
            multi_index = MultiIndex(indices)
            for _, v in data.items():
                try:
                    multi_index.insert({
                        'cik': v['cik_str'],
                        'ticker': v['ticker'],
                        'name': v['title']
                    })
                except Exception:
                    continue
            
            # cache item
            self._add_cache('get_cusips', 'all', multi_index, 
                            expiry_delta=self.cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_cusips: ' + str(e))
            return None