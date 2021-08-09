from datetime import timedelta
import requests

from src.utils.functional.identifiers import check_ticker, to_string, parse_cik
from src.api.base import BaseAPIConnector
from src.utils.mindex import MultiIndex


class RankAndFiledAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

    def get_tickers(self,
                    no_cache: bool=False,
                    cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:
        
        """
            Returns a multi-index with the following fields for all 
            legal US tickers:

            - ticker (index)
            - cik
            - name
            - irs_number
            - exchange
            - sic
            - headquartered_in
            - incorporated_in
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_tickers', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_tickers from cache.')
                    return cached_item
            
            # get tickers data
            self.logger.info('Loading get_tickers from cloud.')
            response = requests.get(self.api_domain + 'cik_ticker.csv')
            response.raise_for_status()          
            content = str(response.content.decode('utf-8'))

            # parse csv data
            lines = content.splitlines()
            data = [line.split('|') for line in lines]
            data = data[1:]
            
            # build multi-index
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for row in data:
                try:

                    cik = parse_cik(to_string(row[0]))
                    ticker = to_string(row[1])
                    name = to_string(row[2])
                    exchange = to_string(row[3])
                    if len(cik) == 0: continue
                    elif not check_ticker(ticker): continue
                    elif len(name) == 0: continue
                    elif len(exchange) == 0: continue
                    elif exchange.startswith('OTC'): continue
                    else:
                        multi_index.insert({
                            'cik': parse_cik(to_string(row[0])),
                            'ticker': to_string(row[1]),
                            'name': to_string(row[2]),
                            'exchange': to_string(row[3]),
                            'sic': to_string(row[4]),
                            'headquartered_in': to_string(row[5]),
                            'incorporated_in': to_string(row[6]),
                            'irs_number': to_string(row[7]),
                        })

                except Exception:
                    continue

            # cache item
            if not no_cache:
                self._add_cache('get_tickers', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_tickers: ' + str(e))
            return None

    def get_industries(self,
                       no_cache: bool=False,
                       cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal SIC types:

            - sic (index)
            - sic_classification
            - naics
            - naics_classification
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_industries', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_industries from cache.')
                    return cached_item
                
            # get industries data
            self.logger.info('Loading get_industries from cloud.')
            response = requests.get(self.api_domain + 'sic_naics.csv')
            response.raise_for_status()          
            content = str(response.content.decode('utf-8'))

            # parse csv data
            lines = content.splitlines()
            data = [line.split('|') for line in lines]
            data = data[1:]
            
            # build multi-index
            indices = ['sic']
            multi_index = MultiIndex(indices, default_index_key='sic', safe_mode=True)
            for row in data:
                try:

                    sic = row[0]
                    if len(sic) == 0: continue
                    else:
                        multi_index.insert({
                            'sic': to_string(row[0]),
                            'sic_classification': to_string(row[1]),
                            'naics': to_string(row[2]),
                            'naics_classification': to_string(row[3])
                        })

                except Exception:
                    continue
        
            # cache item
            if not no_cache:
                self._add_cache('get_industries', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_industries: ' + str(e))
            return None

    def get_cusips(self,
                   no_cache: bool=False,
                   cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal ticker types:

            - ticker (index)
            - cik
            - issuer
            - cusip
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_cusips', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_cusips from cache.')
                    return cached_item
                
            # get cusips data
            self.logger.info('Loading get_cusips from cloud.')
            response = requests.get(self.api_domain + 'cusip_ticker.csv')
            response.raise_for_status()          
            content = str(response.content.decode('utf-8'))

            # parse csv data
            lines = content.splitlines()
            data = [line.split('|') for line in lines]
            data = data[1:-1]
            
            # build multi-index
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for row in data:
                try:

                    cik = parse_cik(to_string(row[3]))
                    ticker = to_string(row[1])
                    issuer = to_string(row[0])
                    cusip = to_string(row[2])
                    if len(cik) == 0: continue
                    elif not check_ticker(ticker): continue
                    elif len(issuer) == 0: continue
                    elif len(cusip) == 0: continue
                    else:
                        multi_index.insert({
                            'cik': parse_cik(to_string(row[3])),
                            'ticker': to_string(row[1]),
                            'issuer': to_string(row[0]),
                            'cusip': to_string(row[2])
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
    
    def get_leis(self,
                 no_cache: bool=False,
                 cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal ticker types:

            - cik (index)
            - name
            - lei
            - legal_form
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_leis', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_leis from cache.')
                    return cached_item
            
            # get leis data
            self.logger.info('Loading get_leis from cloud.')
            response = requests.get(self.api_domain + 'cik_lei.csv')
            response.raise_for_status()          
            content = str(response.content.decode('utf-8'))

            # parse csv data
            lines = content.splitlines()
            data = [line.split('|') for line in lines]
            data = data[1:]
            
            # build multi-index
            indices = ['cik']
            multi_index = MultiIndex(indices, default_index_key='cik', safe_mode=True)
            for row in data:
                try:

                    cik = parse_cik(to_string(row[0]))
                    name = to_string(row[1])
                    lei = to_string(row[2])
                    if len(cik) == 0: continue
                    elif len(name) == 0: continue
                    elif len(lei) == 0: continue
                    else:
                        multi_index.insert({
                            'cik': parse_cik(to_string(row[0])),
                            'name': to_string(row[1]),
                            'lei': to_string(row[2]),
                            'legal_form': to_string(row[3])
                        })

                except Exception:
                    continue
                    
            # cache item
            if not no_cache:
                self._add_cache('get_leis', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_leis: ' + str(e))
            return None
