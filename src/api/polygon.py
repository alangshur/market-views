from datetime import timezone, timedelta
from dateutil import parser
from typing import Any
from tqdm import tqdm
import requests
import time

from src.utils.functional.identifiers import check_ticker
from src.utils.mindex import MultiIndex
from src.api.base import BaseAPIConnector


class PolygonAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

    def get_market_status(self) -> dict:
        try:
            return self._query_endpoint(
                endpoint_name='now',    
                alt_domain=self.api_credentials['api-domain-status'],
                check_ok=False
            )

        except Exception as e:
            self.logger.exception('Error in get_market_status: ' + str(e))
            return None
    
    def get_upcoming_holidays(self) -> list:
        try:
            return self._query_endpoint(
                endpoint_name='upcoming',    
                alt_domain=self.api_credentials['api-domain-status'],
                check_ok=False
            )

        except Exception as e:
            self.logger.exception('Error in get_upcoming_holidays: ' + str(e))
            return None
    
    def get_stock_splits(self, ticker: str) -> list:
        try:
            assert(len(ticker) > 0)
            result = self._query_endpoint(
                endpoint_name='splits/' + ticker,    
                alt_domain=self.api_credentials['api-domain-v2']
            )

            if len(result) > 0: return result
            else: return None

        except Exception as e:
            self.logger.exception('Error in get_ticker_types: ' + str(e))
            return None

    def get_stock_dividends(self, ticker: str) -> list:
        try:
            assert(len(ticker) > 0)
            result = self._query_endpoint(
                endpoint_name='dividends/' + ticker,    
                alt_domain=self.api_credentials['api-domain-v2']
            )

            if len(result) > 0: return result
            else: return None

        except Exception as e:
            self.logger.exception('Error in get_ticker_types: ' + str(e))
            return None

    def get_ticker_types(self) -> dict:
        try:
            return self._query_endpoint(
                endpoint_name='types',    
                alt_domain=self.api_credentials['api-domain-v2']
            )

        except Exception as e:
            self.logger.exception('Error in get_ticker_types: ' + str(e))
            return None
    
    def get_markets(self) -> list:
        try:
            return self._query_endpoint(
                endpoint_name='markets',    
                alt_domain=self.api_credentials['api-domain-v2']
            )

        except Exception as e:
            self.logger.exception('Error in get_markets: ' + str(e))
            return None

    def get_locales(self) -> list:
        try:
            return self._query_endpoint(
                endpoint_name='locales',
                alt_domain=self.api_credentials['api-domain-v2']
            )

        except Exception as e:
            self.logger.exception('Error in get_locales: ' + str(e))
            return None

    def get_internal_exchanges(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            internal ticker types:

            - mic (index)
            - name
            - type
            - market
            - tape_id
        """
        
        try:
            exchanges_data = self._query_endpoint(
                endpoint_name='exchanges',    
                alt_domain=self.api_credentials['api-domain-meta'],
                check_ok=False
            )

            # post-process data
            indices = ['mic']
            multi_index = MultiIndex(indices, default_index_key='mic', safe_mode=True)
            for exchange_data in exchanges_data:
                try:
                    multi_index.insert({
                        'mic': exchange_data['mic'],
                        'name': exchange_data['name'],
                        'type': exchange_data['type'],
                        'market': exchange_data['market'],
                        'tape_id': exchange_data['tape'],
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_exchanges: ' + str(e))
            return None
    
    def get_internal_tickers(self,
                             max_iters: int=15) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - name
            - locale
            - asset_class
            - exchange_mic
            - currency_code
            - figi
            - last_updated
        """

        try:
            tickers_data = []

            # send initial request
            response = requests.get(
                url=self.api_domain + 'tickers', 
                params={
                    'apiKey': self.api_key,
                    'limit': 1000,
                    'active': True,
                    'type': 'CS',
                    'market': 'stocks'
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'OK')
            assert(json_response['count'] > 0)
            assert(len(json_response['next_url']) > 0)
            tickers_data.extend(json_response['results'])
            next_url = json_response['next_url']

            # iteratively request cursor
            iters = 0
            while True:
            
                # send initial request
                response = requests.get(
                    url=next_url, 
                    headers={
                        'Authorization': 'Bearer ' + self.api_key
                    }
                )

                # verify response
                response.raise_for_status()
                json_response = response.json()
                assert(json_response['status'] == 'OK')
                assert(json_response['count'] > 0)
                tickers_data.extend(json_response['results'])

                # check next url
                if 'next_url' not in json_response: 
                    break
                elif iters >= max_iters: 
                    raise Exception('max iterations exceeded')
                else: 
                    next_url = json_response['next_url']
                    iters += 1

            # post-process data
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker_data in tickers_data:
                try:

                    # check ticker
                    if not check_ticker(ticker_data['ticker']):
                        continue

                    # insert ticker
                    multi_index.insert({
                        'ticker': ticker_data['ticker'],
                        'name': ticker_data['name'],
                        'locale': ticker_data['locale'].upper(),
                        'figi': None if 'composite_figi' not in ticker_data else ticker_data['composite_figi'],
                        'asset_class': 'stocks',
                        'exchange_mic': ticker_data['primary_exchange'],
                        'currency_code': ticker_data['currency_name'].upper(),
                        'last_updated': parser.parse(ticker_data['last_updated_utc']).astimezone(timezone.utc).isoformat(),
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_internal_tickers: ' + str(e))
            return None
    
    def get_internal_ticker_details(self,
                                    no_cache: bool=False,
                                    cache_expiry_delta: timedelta=timedelta(days=30),
                                    progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - name
            - cik
            - figi
            - lei
            - bloomberg
            - sic
            - sector
            - industry
            - country
            - list_date
            - ceo
            - phone
            - employees
            - url
            - description
            - hq_address
            - hq_state
            - hq_country
        """

        try: 

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_internal_ticker_details', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_details from cache.')
                    return cached_item

            # get internal tickers
            internal_tickers = self.get_internal_tickers()
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_ticker_details from internet.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:

                    # query ticker details
                    ticker_details = self._query_endpoint(
                        endpoint_name='symbols/{}/company'.format(ticker),    
                        alt_domain=self.api_credentials['api-domain-meta'],
                        check_ok=False
                    )

                    # insert indices
                    multi_index.insert({
                        'ticker': ticker_details['symbol'],
                        'name': ticker_details['name'],
                        'cik': ticker_details['cik'],
                        'figi': ticker_details['figi'],
                        'lei': ticker_details['lei'],
                        'bloomberg': ticker_details['bloomberg'],
                        'sic': ticker_details['sic'],
                        'sector': ticker_details['sector'],
                        'industry': ticker_details['industry'],
                        'country': ticker_details['country'].upper(),
                        'list_date': ticker_details['listdate'],
                        'ceo': ticker_details['ceo'],
                        'phone': ticker_details['phone'],
                        'employees': ticker_details['employees'],
                        'url': ticker_details['url'],
                        'description': ticker_details['description'],
                        'hq_address': ticker_details['hq_address'],
                        'hq_state': ticker_details['hq_state'],
                        'hq_country': ticker_details['hq_country']
                    })

                except Exception:
                    pass
                
            # cache item
            if not no_cache:
                self._add_cache('get_internal_ticker_details', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_details: ' + str(e))
            return None

    def query_ticker_with_cusip(self, cusip: str) -> str:
        try:

            # send requests
            attempts_count = 0
            while True:
                json_response = self._query_tickers_endpoint({
                    'apiKey': self.api_key,
                    'cusip': cusip,
                    'limit': 1
                })

                if json_response is None and attempts_count > 3:
                    raise Exception('query attempts failed')
                elif json_response is None:
                    attempts_count += 1
                    time.sleep(1)
                else:
                    break
    
            # extract ticker
            if json_response['results'] is None or len(json_response['results']) == 0: ticker = ''
            else: ticker = str(json_response['results'][0]['ticker'])
            if not check_ticker(ticker): ticker = ''
            return ticker

        except Exception as e:
            self.logger.exception('Error in query_ticker_with_cusip: ' + str(e))
            return None
    
    def _query_tickers_endpoint(self, params: dict) -> dict: 
        try:
            response = requests.get(
                url=self.api_domain + 'tickers', 
                params=params
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'OK')
            return json_response

        except Exception:
            return None

    def _query_endpoint(self, endpoint_name: str,
                        alt_domain: str=None,
                        check_ok: bool=True) -> Any: 

        # send no-param request
        if alt_domain is None: domain = self.api_domain
        else: domain = alt_domain
        response = requests.get(
            url=domain + endpoint_name, 
            params={'apiKey': self.api_key}
        )

        # verify response
        response.raise_for_status()
        json_response = response.json()
        if check_ok:
            assert(json_response['status'] == 'OK')
            return json_response['results']
        else:
            return json_response