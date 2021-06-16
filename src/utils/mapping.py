import json
import requests


class MappingModule:

    def __init__(self, logger,
                 sec_tickers_mapping_url='https://www.sec.gov/files/company_tickers.json',
                 sec_tickers_mapping_path='meta/sec/tickers.json'):

        try:

            # download SEC tickers mapping
            response = requests.get(sec_tickers_mapping_url)
            tickers_data = response.json()
            assert(len(tickers_data) > 0)
            logger.info('Downloaded SEC tickers mapping.', flush=True)

        except:

            # load local SEC tickers mapping
            f = open(sec_tickers_mapping_path, 'r')
            tickers_data = json.load(f)
            f.close()
            logger.info('Loaded SEC tickers mapping locally.', flush=True) 