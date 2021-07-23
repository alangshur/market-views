from datetime import datetime, timezone
from requests_html import HTMLSession
from bs4 import BeautifulSoup
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

                # skip missing holdings
                if 'holdings' not in filing or 'periodOfReport' not in filing:
                    continue

                # clean holdings data
                cleaned_holdings = []
                for holding in filing['holdings']:
                    cusip = str(holding['cusip']).upper()
                    if len(cusip) != 9: 
                        self.logger.error('Bad CUSIP encountered: {}.'.format(cusip))
                        continue

                    # query ticker from cusip
                    ticker = polygon_connector.query_ticker_with_cusip(cusip)
                    if ticker is None: ticker = ''

                    cleaned_holdings.append({
                        'issuer_name': holding['nameOfIssuer'],
                        'cusip': cusip,
                        'ticker': ticker,
                        'value': float(holding['value']),
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

    def query_form_4_filings(self, fetch_from_dt: datetime) -> Tuple[list, datetime]:
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
                        'query': 'formType:\"4\" AND filedAt:{' + fetch_from_str + ' TO ' + fetch_until_str + '}'
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

        except Exception as e:
            self.logger.exception('Error in query_form_4_filings: ' + str(e))
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
    
    def _fetch_form_4_xml_data(self, xml_url: str) -> dict:

        # query xml form
        session = HTMLSession()
        r = session.get(xml_url)
        r.raise_for_status()

        # parse xml
        content = r.content.decode('utf-8')
        content = BeautifulSoup(content, features='lxml')
        
        # get form data
        form_data = {}
        form_data['id'] = str(uuid.uuid4().hex)
        form_data['report_period'] = content.find('periodofreport').get_text()

        # get issuer data
        form_data['issuer'] = {}
        issuer_information = content.find('issuer')
        form_data['issuer']['cik'] = issuer_information.find('issuercik').get_text()
        form_data['issuer']['ticker'] = issuer_information.find('issuertradingsymbol').get_text()
        form_data['issuer']['name'] = issuer_information.find('issuername').get_text()

        # get reporting owner data
        form_data['reporting_owner'] = {}
        reporting_owner_information = content.find('reportingowner')
        form_data['reporting_owner']['cik'] = reporting_owner_information.find('rptownercik')
        form_data['reporting_owner']['name'] = reporting_owner_information.find('rptownername')
        form_data['reporting_owner']['is_director'] = bool(int(reporting_owner_information.find('isdirector').get_text()))
        form_data['reporting_owner']['is_officer'] = bool(int(reporting_owner_information.find('isofficer').get_text()))
        form_data['reporting_owner']['is_ten_percent_owner'] = bool(int(reporting_owner_information.find('istenpercentowner').get_text()))
        form_data['reporting_owner']['is_other'] = bool(int(reporting_owner_information.find('isother').get_text()))

        # get non-derivative data
        non_derivative_transactions = []
        non_derivative_table = content.find('nonderivativetable')
        if non_derivative_table is not None and len(non_derivative_table.get_text()) > 0:

            # iterate over non-derivative transactions
            transactions = non_derivative_table.findAll('nonderivativetransaction')
            for transaction in transactions:
                non_derivative_transactions.append({
                    'security_title': transaction.find('securitytitle').find('value').get_text(),
                    'date': transaction.find('transactiondate').find('value').get_text(),
                    'shares': self._parse_int((transaction.find('transactionshares').find('value').get_text())),
                    'price': self._parse_float(transaction.find('transactionpricepershare').find('value').get_text()),
                    'type': 'acquired' if transaction.find('transactionacquireddisposedcode').find('value').get_text() == 'A' else 'disposed',
                    'post_transaction_shares': self._parse_int((transaction.find('sharesownedfollowingtransaction').find('value').get_text())),
                    'nature_of_ownership': '' if transaction.find('natureofownership') is None else transaction.find('natureofownership').find('value').get_text() 
                })
        
        # add non-derivative transactions
        form_data['non_derivative_transactions'] = non_derivative_transactions

        # get derivative data
        derivative_transactions = []
        derivative_table = content.find('derivativetable')
        if derivative_table is not None and len(derivative_table.get_text()) > 0:
            
            # iterate over derivative transactions
            transactions = non_derivative_table.findAll('derivativetransaction')
            for transaction in transactions:
                derivative_transactions.append({
                    'security_title': transaction.find('securitytitle').find('value').get_text(),
                    'date': transaction.find('transactiondate').find('value').get_text(),
                    'shares': self._parse_int((transaction.find('transactionshares').find('value').get_text())),
                    'price': self._parse_float(transaction.find('transactionpricepershare').find('value').get_text()),
                    'type': 'acquired' if transaction.find('transactionacquireddisposedcode').find('value').get_text() == 'A' else 'disposed',
                    'underlying_security_title': transaction.find('underlyingsecuritytitle').find('value').get_text(),
                    'underlying_shares': self._parse_int(transaction.find('underlyingsecurityshares').find('value').get_text()),
                    'post_transaction_shares': self._parse_int((transaction.find('sharesownedfollowingtransaction').find('value').get_text())),
                    'nature_of_ownership': '' if transaction.find('natureofownership') is None else transaction.find('natureofownership').find('value').get_text() 
                })

        # add derivative transactions
        form_data['derivative_transactions'] = derivative_transactions

        return form_data
        
    def _parse_int(self, value: str) -> int:
        try: return int(value)
        except Exception: return None

    def _parse_float(self, value: str) -> float:
        try: return float(value)
        except Exception: return None

    

        