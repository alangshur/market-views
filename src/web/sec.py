import requests


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

        
