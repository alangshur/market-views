import requests

from src.api.base import APIBaseConnector


class APIPolygonConnector(APIBaseConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)
    
    def query_ticker_with_cusip(self, cusip: str) -> str:
        try:

            # check cache
            ticker = self._get_cache('cusip_to_ticker', cusip)
            if ticker is not None: return ticker

            # send request 
            response = requests.get(
                url=self.api_domain + 'tickers', 
                params={
                    'apiKey': self.api_key,
                    'cusip': cusip,
                    'limit': 2
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'OK')

            # extract ticker
            if json_response['results'] is None or len(json_response['results']) != 1: ticker = None
            else: ticker = str(json_response['results'][0]['ticker'])

            # cache ticker
            if ticker is not None:
                self._add_cache('cusip_to_ticker', cusip, ticker)
            
            return ticker

        except Exception as e:
            self.logger.exception('Error in query_ticker_with_cusip: ' + str(e))
            return None


