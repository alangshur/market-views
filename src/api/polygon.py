from datetime import timezone, timedelta
from dateutil import parser
from typing import Any
from tqdm import tqdm
import requests
import time

from src.utils.functional.identifiers import check_ticker, to_string, parse_cik
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
            assert(len(ticker) > 0, 'no tickers specified')
            result = self._query_endpoint(
                endpoint_name='splits/' + ticker,    
                alt_domain=self.api_credentials['api-domain-v2']
            )

            if len(result) > 0: return result
            else: return None

        except Exception as e:
            self.logger.exception('Error in get_stock_splits: ' + str(e))
            return None

    def get_stock_dividends(self, ticker: str) -> list:
        try:
            assert(len(ticker) > 0, 'no tickers specified')
            result = self._query_endpoint(
                endpoint_name='dividends/' + ticker,    
                alt_domain=self.api_credentials['api-domain-v2']
            )

            if len(result) > 0: return result
            else: return None

        except Exception as e:
            self.logger.exception('Error in get_stock_dividends: ' + str(e))
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
            self.logger.info('Loading get_internal_exchanges from cloud.')
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
                        'mic': to_string(exchange_data['mic']),
                        'name': to_string(exchange_data['name']),
                        'type': exchange_data['type'],
                        'market': exchange_data['market'],
                        'tape_id': exchange_data['tape'],
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_internal_exchanges: ' + str(e))
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
            self.logger.info('Loading get_internal_tickers from cloud.')
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
            assert(json_response['status'] == 'OK', 'bad response status')
            assert(json_response['count'] > 0, 'no data in response')
            assert(len(json_response['next_url']) > 0, 'no next_url in response')
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
                assert(json_response['status'] == 'OK', 'bad response status')
                assert(json_response['count'] > 0, 'no data in response')
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
                        'ticker': to_string(ticker_data['ticker']),
                        'name': to_string(ticker_data['name']),
                        'figi': None if 'composite_figi' not in ticker_data else to_string(ticker_data['composite_figi']),
                        'locale': to_string(ticker_data['locale']).upper(),
                        'asset_class': 'stocks',
                        'exchange_mic': to_string(ticker_data['primary_exchange']),
                        'currency_code': to_string(ticker_data['currency_name']).upper(),
                        'last_updated': parser.parse(ticker_data['last_updated_utc']).astimezone(timezone.utc).isoformat(),
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_internal_tickers: ' + str(e))
            return None
    
    def get_internal_ticker_quotes(self,
                                   no_cache: bool=False,
                                   cache_expiry_delta: timedelta=timedelta(days=30),
                                   progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
        """

        try: 

            # get internal tickers
            internal_tickers = self.get_internal_tickers()
            internal_tickers = internal_tickers.get_all_key_values('ticker')
            internal_tickers_str = ','.join(internal_tickers)

            # query ticker quotes
            self.logger.info('Loading get_internal_ticker_quotes from cloud.')
            response = requests.get(
                url=self.api_credentials['api-domain-snapshot'] + 'tickers', 
                params={
                    'apiKey': self.api_key,
                    'tickers': internal_tickers_str
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'DELAYED', 'bad response status')
            
            # check cache if empty response
            if len(json_response['count']) == 0 and not no_cache:
                cached_item = self._get_cache('get_internal_ticker_quotes', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_quotes from cache.')
                    return cached_item
                else:
                    raise Exception('no data in response')
            elif len(json_response['count']) == 0:
                raise Exception('no data in response')
            elif len(json_response['count']) != len(internal_tickers):
                raise Exception('missing data in response')

            # get ticker quotes data
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in json_response['tickers']:

                # TODO: implement ticker quotes
                pass

            # cache item
            if not no_cache:
                self._add_cache('get_internal_ticker_quotes', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_quotes: ' + str(e))
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
            self.logger.info('Loading get_internal_ticker_details from cloud.')
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
                        'ticker': to_string(ticker_details['symbol']),
                        'name': to_string(ticker_details['name']),
                        'cik': parse_cik(to_string(ticker_details['cik'])),
                        'figi': to_string(ticker_details['figi']),
                        'lei': to_string(ticker_details['lei']),
                        'bloomberg': to_string(ticker_details['bloomberg']),
                        'sic': to_string(ticker_details['sic']),
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

    def get_internal_ticker_dividends(self, 
                                      no_cache: bool=False,
                                      cache_expiry_delta: timedelta=timedelta(days=30),
                                      progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
        """

        try: 

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_internal_ticker_dividends', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_dividends from cache.')
                    return cached_item

            # get internal tickers
            internal_tickers = self.get_internal_tickers()
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_ticker_dividends from cloud.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:

                    # query ticker details
                    ticker_dividends = self._query_endpoint(
                        endpoint_name='dividends/{}'.format(ticker),    
                        alt_domain=self.api_credentials['api-domain-v2']
                    )
                    
                    # get last year dividends
                    idx = 0
                    annual_dividends = []
                    cur_dividend_date = ticker_dividends[idx]['paymentDate']
                    cur_dividend_date = parser.parse(cur_dividend_date).date()
                    start_dividend_date = cur_dividend_date - timedelta(days=400)
                    while cur_dividend_date > start_dividend_date:
                        annual_dividends.append(ticker_dividends[idx]['amount'])
                        idx += 1
                        cur_dividend_date = ticker_dividends[idx]['paymentDate']
                        cur_dividend_date = parser.parse(cur_dividend_date).date()

                    # get dividend yield
                    if len(annual_dividends) > 6:
                        annual_dividends = annual_dividends[:12]
                        rolling_annual_dividend = sum(annual_dividends)
                        annual_dividend = annual_dividends[0] * 12
                    else:
                        annual_dividends = annual_dividends[:4]
                        rolling_annual_dividend = sum(annual_dividends)
                        annual_dividend = annual_dividends[0] * 4

                    # TODO: calculate dividend yield and all stats

                except Exception:
                    pass
                
            # cache item
            if not no_cache:
                self._add_cache('get_internal_ticker_dividends', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_dividends: ' + str(e))
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
            else: ticker = to_string(json_response['results'][0]['ticker'])
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
            assert(json_response['status'] == 'OK', 'bad response status')
            return json_response

        except Exception:
            return None

    def _query_endpoint(self, endpoint_name: str,
                        alt_domain: str=None,
                        check_ok: bool=True,
                        additional_params: dict={},
                        delayed_status=False) -> Any: 

        # send no-param request
        if alt_domain is None: domain = self.api_domain
        else: domain = alt_domain
        response = requests.get(
            url=domain + endpoint_name, 
            params={
                'apiKey': self.api_key,
                **additional_params
            }
        )

        # verify response
        response.raise_for_status()
        json_response = response.json()
        if check_ok:
            if delayed_status: assert(json_response['status'] == 'DELAYED', 'bad response status')
            else: assert(json_response['status'] == 'OK', 'bad response status')
            return json_response['results']
        else:
            return json_response