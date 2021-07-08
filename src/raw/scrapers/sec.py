from typing import Tuple, Union
from requests_html import HTMLSession
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from dateutil import parser
import re

from src.raw.scrapers.base import BaseScraperModule


class SEC13FScraper(BaseScraperModule):

    def __init__(self,
                 filing_links_url: str='https://sec.report/loadmore.php',
                 filing_url: str='https://sec.report/Document/'):

        super().__init__(self.__class__.__name__)
        self.filing_links_url = filing_links_url
        self.filing_url = filing_url

    def fetch_filing_ids(self, since_dt: Union[datetime, str], 
                         form_number: str='13F-HR') -> Tuple[list, list, datetime]:
                         
        try:

            # get query dt
            if isinstance(since_dt, datetime): dt = since_dt.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(since_dt, str): dt = since_dt
            else: raise Exception('invalid type for \'since_dt\'')

            # load and render webpage
            session = HTMLSession()
            r = session.get(self.filing_links_url, params={
                'form_number': form_number,
                'dt': dt
            })
            r.raise_for_status()
            r.html.render(timeout=30)
            session.close()

            # parse string into html
            content = r.content.decode('utf-8')
            content_html = BeautifulSoup(content, features='lxml')

            # extract filing ids
            filing_links = [link.get('href') for link in content_html.find_all('a')]
            filing_ids = [re.search('\d{10}-\d{2}-\d{6}', link).group(0) for link in filing_links]
            filing_ids = list(set(filing_ids))

            # extract start/next query dt
            query_dts = content_html.findAll('small')
            query_dts = [parser.parse(dt.get_text()) for dt in query_dts]
            next_query_dt = content_html.find('div', {'id': 'dt'})
            next_query_dt = parser.parse(next_query_dt.text)

            return filing_ids, query_dts, next_query_dt

        except Exception as e:
            self.logger.exception('Exception in fetch_filing_ids: {}.'.format(e))
            return None

    def fetch_filing_data(self, filing_id: str) -> Tuple[dict, list]:
        try:

            # load and render webpage
            session = HTMLSession()
            r = session.get(self.filing_url + filing_id + '/' + filing_id + '.txt')
            r.raise_for_status()
            r.html.render(timeout=30)
            session.close()

            # parse string into html
            content = r.content.decode('utf-8')
            content_html = BeautifulSoup(content, features='lxml')

            # extract filing ids
            xml_filing_data = content_html.findAll('xml')
            if len(xml_filing_data) != 2: return None
            
            # extract filing/holdings data
            filing_data, holdings_data = xml_filing_data
            filing_data = self._extract_filing_data(filing_data)
            holdings_data = self._extract_filing_holdings_data(holdings_data)
            if len(filing_data) == 0: return None
            if len(holdings_data) == 0: return None

            return filing_data, holdings_data

        except Exception as e:
            self.logger.exception('Exception in fetch_filing_data: {}.'.format(e))
            return None

    def _extract_filing_data(self, filing_data: Tag) -> dict:

        # extract report data
        submission_type = filing_data.find('submissiontype').get_text()
        report_period = filing_data.find('periodofreport').get_text()
        form_file_no = filing_data.find('form13ffilenumber').get_text()

        # extract company data
        company_name = filing_data.find('filingmanager').find('name').get_text()
        cik = filing_data.find('cik').get_text()

        # extract holdings summary data
        total_holdings_entries = int(filing_data.find('tableentrytotal').get_text())
        total_holdings_value = int(filing_data.find('tablevaluetotal').get_text()) * 1000

        return {
            'submission_type': submission_type,
            'report_period': report_period,
            'form_file_no': form_file_no,
            'company_name': company_name,
            'cik': cik,
            'total_holdings_entries': total_holdings_entries,
            'total_holdings_value': total_holdings_value
        }

    def _extract_filing_holdings_data(self, holdings_data: Tag) -> list:

        # extract holdings data
        issuer_name_s = holdings_data.findAll('nameofissuer') + holdings_data.findAll('ns1:nameofissuer')
        class_title_s = holdings_data.findAll('titleofclass') + holdings_data.findAll('ns1:titleofclass')
        cusip_s = holdings_data.findAll('cusip') + holdings_data.findAll('ns1:cusip')
        value_s = holdings_data.findAll('value') + holdings_data.findAll('ns1:value')
        shares_s = holdings_data.findAll('sshprnamt') + holdings_data.findAll('ns1:sshprnamt')
        investment_discretion_s = holdings_data.findAll('investmentdiscretion') + holdings_data.findAll('ns1:investmentdiscretion')

        # filter tag data
        filt_holdings = [] 
        for i in range(len(issuer_name_s)):
            filt_holdings.append({
                'issuer_name': issuer_name_s[i].get_text(),
                'class_title': class_title_s[i].get_text(),
                'cusip': cusip_s[i].get_text(),
                'value': int(value_s[i].get_text()) * 1000,
                'shares': int(shares_s[i].get_text()),
                'investment_discretion': investment_discretion_s[i].get_text()
            })

        return filt_holdings