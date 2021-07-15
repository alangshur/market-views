import requests
import time

from src.api.base import BaseAPIConnector


class PolygonAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)
    
    def query_ticker_with_cusip(self, cusip: str) -> str:
        try:

            # check cache
            ticker = self._get_cache('cusip_to_ticker', cusip)
            if ticker is not None: 
                return ticker

            # send requests
            attempts_count = 0
            while True:
                json_response = self._query_tickers_endpoint({
                    'apiKey': self.api_key,
                    'cusip': cusip,
                    'limit': 2
                })

                if json_response is None and attempts_count > 3: 
                    raise Exception('query attempts failed')
                elif json_response is None: 
                    attempts_count += 1
                    time.sleep(1)
                else:
                    break
    
            # extract ticker
            if json_response['results'] is None or len(json_response['results']) != 1: ticker = None
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