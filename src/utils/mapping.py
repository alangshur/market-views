import json
from re import X
import requests

from src.utils.logger import BaseModuleWithLogging


class MappingModule(BaseModuleWithLogging):

    def __init__(self,
                 sec_tickers_mapping_url: str='https://www.sec.gov/files/company_tickers.json',
                 sec_tickers_mapping_path: str='local/sec/tickers.json'):

        super().__init__(self.__class__.__name__)
        self.sec_tickers_mapping_url = sec_tickers_mapping_url
        self.sec_tickers_mapping_path = sec_tickers_mapping_path

        # load mappings
        self.mappings = {}
        self._load_cik_to_ticker_mapping()

    def get_mapping_keys(self) -> list:
        return list(self.mappings.keys())

    def get_mapping(self, mapping_key: str) -> dict:
        if mapping_key in self.mappings: return self.mappings[mapping_key]
        else: return None

    def get_ticker_from_cik(self, cik: str) -> dict:
        return self.mappings['cik_to_ticker'].get(cik)

    def _load_cik_to_ticker_mapping(self) -> None:
        try:

            # download SEC tickers mapping
            response = requests.get(self.sec_tickers_mapping_url)
            tickers_data = response.json()
            assert(len(tickers_data) > 0)
            self.logger.info('Downloaded SEC tickers mapping.')

        except Exception as e:
            self.logger.error('Exception in _load_sec_tickers_mapping: {}.'.format(e))

            # load local SEC tickers mapping
            f = open(self.sec_tickers_mapping_path, 'r')
            tickers_data = json.load(f)
            f.close()
            self.logger.warning('Loaded SEC tickers mapping locally.')

        # build mapping
        cik_to_ticker_mapping = {}
        for v in tickers_data.values():
            cik_to_ticker_mapping[v['cik_str']] = {
                'ticker': v['ticker'],
                'title': v['title']
            }

        self.mappings['cik_to_ticker'] = cik_to_ticker_mapping