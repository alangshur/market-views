import requests

from src.api.base import APIBaseConnector


class APIPolygonConnector(APIBaseConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)
    
    def query_ticker_with_cusip(self, cusip: str) -> dict:
        try:
            
            # send request 
            response = requests.get(self.api_domain, params={
                'apiKey': self.api_key,
                'cusip': cusip,
                'limit': 2
            })

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert(json_response['status'] == 'OK')

            # return response
            if response is None or len(response) != 1:
                return None

        except Exception as e:
            self.logger.exception('Error in query_ticker_with_cusip: ' + str(e))
            return None


