from typing import Any
import requests
import time

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

    def get_exchanges(self) -> list:
        try:
            return self._query_endpoint(
                endpoint_name='exchanges',    
                alt_domain=self.api_credentials['api-domain-meta'],
                check_ok=False
            )

        except Exception as e:
            self.logger.exception('Error in get_exchanges: ' + str(e))
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
    
    def get_all_tickers(self,
                        max_iters: int=50) -> dict:

        try:
            tickers = []

            # send initial request
            response = requests.get(
                url=self.api_domain + 'tickers', 
                params={
                    'apiKey': self.api_key,
                    'limit': 1000,
                    'type': 'CS'
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'OK')
            assert(json_response['count'] > 0)
            assert(len(json_response['next_url']) > 0)
            tickers.extend(json_response['results'])
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
                tickers.extend(json_response['results'])

                # check next url
                if 'next_url' not in json_response: return tickers
                elif iters >= max_iters: raise Exception('max iterations exceeded')
                else: 
                    next_url = json_response['next_url']
                    iters += 1

        except Exception as e:
            self.logger.exception('Error in get_all_tickers: ' + str(e))
            return None

    def query_ticker_with_cusip(self, cusip: str) -> str:
        try:

            # check cache
            ticker = self._get_cache('cusip_to_ticker', cusip)
            if ticker is not None: return ticker
            
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

            # cache ticker
            self._add_cache('cusip_to_ticker', cusip, ticker)
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