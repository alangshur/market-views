from requests_html import HTMLSession
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd
import re


class SEC13FWebScraper:

    def __init__(self):
        
        # try to download tickers data from https://www.sec.gov/files/company_tickers.json
        # otherwise pull local file

        # company:
        #   - name
        #   - address
        #   - telephone number
        #   - state of incorporation
        #   - CIK (central index key) number
        #   - SIC (standard industrial classification) code

        self.filing_links_url = 'https://sec.report/Form/'
        self.filing_url = 'https://sec.report/Document/'

    def fetch_filing_ids(self, type='13F-HR'):

        # load and render webpage
        session = HTMLSession()
        r = session.get(self.filing_links_url + type)
        r.html.render()
        session.close()

        # parse string into html
        content = r.content.decode('utf-8')
        content_html = BeautifulSoup(content)

        # extract filing ids
        filing_table = content_html.find('table')
        filing_links = [link.get('href') for link in filing_table.find_all('a')]
        filing_ids = [re.search('\d{10}-\d{2}-\d{6}', link).group(0) for link in filing_links]
        return filing_ids

    def fetch_filing_data_links(self, filing_id):

        # load and render webpage
        session = HTMLSession()
        r = session.get(self.filing_url + filing_id)
        r.html.render()
        session.close()

        # parse string into html
        content = r.content.decode('utf-8')
        content_html = BeautifulSoup(content)

        # extract filing ids
        filing_links = [link.get('href') for link in content_html.find_all('a')]
        filing_links = [link for link in filing_links if link.endswith('.xml')]
        primary_link, holdings_link = filing_links[0], filing_links[1]
        return primary_link, holdings_link

    def fetch_filing_holdings(self, holdings_link):

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
