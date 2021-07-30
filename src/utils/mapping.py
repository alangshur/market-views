import requests
import luhn

from src.utils.logger import BaseModuleWithLogging
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.utils.mindex import MultiIndex


class MappingModule(BaseModuleWithLogging):

    def __init__(self, polygon_connector: PolygonAPIConnector, 
                 raf_connector: RankAndFiledAPIConnector):

        super().__init__(self.__class__.__name__)

        self.polygon_connector = polygon_connector
        self.raf_connector = raf_connector

    def build_ticker_mappings(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            internal ticker types:

            - ticker (index)
            - name (index)
            - cusip (index)
            - cik (index)
            - isin (index)
            - lei (index)
            - composite_figi (index)
            - share_class_figi (index)
        """

        indices = [
            'ticker', 'name', 'cusip', 'cik', 'isin', 'lei', 
            'composite_figi', 'share_class_figi'
        ]

        try:
        
            # fetch tickers
            tickers_data = self.polygon_connector.get_all_tickers()
            if tickers_data is None:
                raise Exception('missing tickers data')

            # fetch RAF tickers
            raf_tickers_data = self.raf_connector.get_tickers()
            if tickers_data is None:
                raise Exception('missing RAF tickers data')

            # fetch RAF cusips
            raf_cusips_data = self.raf_connector.get_cusips()
            if raf_cusips_data is None:
                raise Exception('missing RAF cusips data')

            # fetch RAF leis
            raf_leis_data = self.raf_connector.get_leis()
            if raf_leis_data is None:
                raise Exception('missing RAF leis data')

            # build multi-index
            multi_index = MultiIndex(indices, default_index_key='ticker')
            for ticker_data in tickers_data:

                # fetch RAF data
                raf_ticker_data = raf_tickers_data.get('ticker', ticker_data['ticker'])
                raf_cusip_data = raf_cusips_data.get('ticker', ticker_data['ticker'])
                raf_lei_data = raf_leis_data.get('cik', raf_ticker_data['cik'])

                # build ISIN
                country_code = ticker_data['locale'].upper()
                ascii_code_prefix = ''.join([str(ord(c) - 55) for c in country_code])
                isin_checksum_value = ascii_code_prefix + raf_cusip_data['cusip']
                isin_checksum_digit = str(luhn.generate(isin_checksum_value))
                isin = country_code + raf_cusip_data['cusip'] + isin_checksum_digit
                
                # # build index
                # multi_index.insert({
                #     'ticker': ticker_data['ticker'],
                #     'name': raf_ticker_data['name'],
                #     'cusip': raf_cusip_data['cusip'],
                #     'cik': raf_ticker_data['cik'],
                #     'isin': isin,
                #     'lei': raf_lei_data['lei'],
                #     'composite_figi': ticker_data['composite_figi'],
                #     'share_class_figi': ticker_data['share_class_figi']
                # })

            return multi_index

        except Exception as e:
            self.logger.exception('Error in build_ticker_mappings: ' + str(e))
            return None


    def build_exchange_mappings(self) -> dict:
        raise NotImplementedError

    def build_locale_mappings(self) -> dict:
        raise NotImplementedError