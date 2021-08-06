from datetime import timedelta, date
from io import BytesIO
import requests
import zipfile

from src.utils.functional.identifiers import check_ticker, to_string
from src.api.base import BaseAPIConnector
from src.utils.mindex import MultiIndex


class SECGovAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

    def get_ciks(self,
                 no_cache: bool=False,
                 cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal ticker types from SEC EGDAR:

            - ticker (index)
            - cik
            - name
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_ciks', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_ciks from cache.')
                    return cached_item
            
            # get ciks data
            self.logger.info('Loading get_ciks from internet.')
            response = requests.get(self.api_domain + 'company_tickers.json')
            response.raise_for_status()
            data = response.json()
            
            # build multi-index
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for _, v in data.items():
                try:

                    # check ticker
                    if not check_ticker(v['ticker']):
                        continue

                    # insert ticker
                    multi_index.insert({
                        'ticker': to_string(v['ticker']),
                        'cik': to_string(v['cik_str']),
                        'name': to_string(v['title'])
                    })

                except Exception:
                    continue
            
            # cache item
            if not no_cache:
                self._add_cache('get_ciks', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_ciks: ' + str(e))
            return None

    def get_cusips(self,
                   no_cache: bool=False,
                   cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal ticker types from sec.gov:

            - ticker (index)
            - cusip
            - name
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_cusips', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_cusips from cache.')
                    return cached_item

            # get date code
            cur_date = date.today() - timedelta(days=90)
            date_code = cur_date.strftime('%Y%ma')
            query_code = 'data/fails-deliver-data/cnsfails' + date_code + '.zip'

            # get cusips data
            self.logger.info('Loading get_cusips from internet.')
            response = requests.get(self.api_domain + query_code)
            response.raise_for_status()   
            
            # parse ZIP file
            fp = BytesIO(response.content)
            zip_fp = zipfile.ZipFile(fp, 'r')
            data_fp = zip_fp.open(zip_fp.namelist()[0])
            lines = data_fp.read().decode('utf-8').splitlines()
            data = [line.split('|') for line in lines]
            data = data[1:-2]
            zip_fp.close()
            
            # build multi-index
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for row in data:
                try:

                    # check ticker
                    if not check_ticker(row[2]):
                        continue

                    # insert ticker
                    multi_index.insert({
                        'ticker': to_string(row[2]),
                        'cusip': to_string(row[1]),
                        'name': to_string(row[4])
                    })
                    
                except Exception:
                    continue
            
            # cache item
            if not no_cache:
                self._add_cache('get_cusips', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_cusips: ' + str(e))
            return None