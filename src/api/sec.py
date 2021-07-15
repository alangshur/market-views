from datetime import date, datetime, timezone
from dateutil import parser
from typing import Tuple
import requests
import uuid

from src.api.base import BaseAPIConnector
from src.api.polygon import PolygonAPIConnector


class SECAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)
        
    def query_13f_filings(self, fetch_from_dt: datetime,
                          polygon_connector: PolygonAPIConnector) -> Tuple[list, datetime]:

        try:

            # format fetch from dt
            fetch_from_dt = fetch_from_dt.astimezone(timezone.utc)
            fetch_from_str = fetch_from_dt.isoformat()

            # format fetch until dt
            fetch_until_dt = datetime.now(timezone.utc)
            fetch_until_str = fetch_until_dt.isoformat()
            
            # send request
            json_response = self._query_filings_endpoint({
                'query': {
                    'query_string': {
                        'query': 'formType:\"13F\" AND filedAt:{' + fetch_from_str + ' TO ' + fetch_until_str + '}'
                    }
                },
                'from': '0',
                'size': '200',
                'sort': [{
                    'filedAt': {
                        'order': 'asc'
                    }
                }]
            })

            # verify response
            if json_response is None:
                raise Exception('query attempt failed')
            elif len(json_response['filings']) == 0:
                return [], fetch_from_dt

            # get next fetch from dt
            last_dt_str = json_response['filings'][-1]['filedAt']
            next_fetch_from_dt = parser.parse(last_dt_str)
            next_fetch_from_dt = next_fetch_from_dt.astimezone(timezone.utc)

            # clean filings
            cleaned_filings = []
            for filing in json_response['filings']:

                # clean holdings data
                cleaned_holdings = []
                if 'holdings' in filing:
                    for holding in filing['holdings']:
                        ticker = polygon_connector.query_ticker_with_cusip(holding['cusip'])
                        if ticker is None: ticker = ""
                        cleaned_holdings.append({
                            'issuer_name': holding['nameOfIssuer'],
                            'cusip': holding['cusip'],
                            'ticker': ticker,
                            'value': int(holding['value']) * 1000,
                            'shares': int(holding['shrsOrPrnAmt']['sshPrnamt'])
                        })

                # clean filing data
                cleaned_filings.append({
                    'id': str(uuid.uuid4().hex),
                    'accession_number': filing['accessionNo'],
                    'cik': filing['cik'],
                    'ticker': filing['ticker'],
                    'company_name': filing['companyName'],
                    'company_name_long': filing['companyNameLong'],
                    'form_type': filing['formType'],
                    'filed_at': parser.parse(filing['filedAt']).astimezone(timezone.utc).isoformat(),
                    'report_period': filing['periodOfReport'],
                    'holdings': cleaned_holdings
                })

            return cleaned_filings, next_fetch_from_dt

        except Exception as e:
            self.logger.exception('Error in query_13f_filings: ' + str(e))
            return None


    def _query_filings_endpoint(self, json_body: dict) -> dict: 
        try:
            response = requests.post(
                url=self.api_domain,
                params={'token': self.api_key},
                json=json_body
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            return json_response

        except Exception:
            return None