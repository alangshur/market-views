from typing import Tuple, Optional, Union
from requests_html import HTMLSession
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime
import re

from src.utils.logger import BaseModule


class SEC13FWebScraper(BaseModule):

    def __init__(self, mapping_module,
                 filing_links_url: str='https://sec.report/loadmore.php',
                 filing_url: str='https://sec.report/Document/'):

        super().__init__(self.__class__.__name__)
        self.mapping_module = mapping_module
        self.filing_links_url = filing_links_url
        self.filing_url = filing_url

    def fetch_filing_ids(self, since: Union[datetime, str], 
                         form_number: str='13F-HR') -> Optional[Tuple[list, str]]:

        try:

            # get query dt
            if isinstance(since, datetime): dt = since.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(since, str): dt = since
            else: raise Exception('invalid type for \'since\'')

            # load and render webpage
            session = HTMLSession()
            r = session.get(self.filing_links_url, params={
                'form_number': form_number,
                'dt': dt
            })
            r.raise_for_status()
            r.html.render()
            session.close()

            # parse string into html
            content = r.content.decode('utf-8')
            content_html = BeautifulSoup(content, features='lxml')

            # extract filing ids
            filing_links = [link.get('href') for link in content_html.find_all('a')]
            filing_ids = [re.search('\d{10}-\d{2}-\d{6}', link).group(0) for link in filing_links]
            filing_ids = list(set(filing_ids))

            # extract next query dt
            next_query_dt = content_html.find('div', {'id': 'dt'})
            next_query_dt = next_query_dt.text

            return filing_ids, next_query_dt

        except Exception as e:
            self.logger.error('Exception in fetch_filing_ids: {}.'.format(e))
            return None

    def fetch_filing_data(self, filing_id: str) -> Optional[Tuple[dict, dict]]:
        try:

            # load and render webpage
            session = HTMLSession()
            r = session.get(self.filing_url + filing_id + '/' + filing_id + '.txt')
            r.html.render()
            session.close()

            # parse string into html
            content = r.content.decode('utf-8')
            content_html = BeautifulSoup(content, features='lxml')

            # extract filing ids
            xml_filing_data = content_html.findAll('xml')
            if len(xml_filing_data) != 2: raise Exception('unexpected number of xml documents')
    
            # TODO: parse both xml tables

        except Exception as e:
            self.logger.error('Exception in fetch_filing_data_links: {}.'.format(e))
            return None

    def fetch_filing_holdings(self, holdings_link: str) -> Optional[list]:

        # load and render webpage
        session = HTMLSession()
        r = session.get(holdings_link)
        r.html.render()
        session.close()

        # parse xml        
        tree = ET.parse(r.content.decode('utf-8'))
        root = tree.getroot()

        # load holdings data
        holdings_data = []
        for holding in root:
            holding_data = {
                'issuer_name': holding[0],
                'class_title': holding[1],
                'cusip': holding[2],
                'value': float(holding[3]) * 100,
                'shares': int(holding[4][0])
            }
