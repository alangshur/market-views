import requests

from src.utils.logger import BaseModuleWithLogging
from src.api.polygon import PolygonAPIConnector


class MappingModule(BaseModuleWithLogging):

    def __init__(self, polygon_connector: PolygonAPIConnector):
        super().__init__(self.__class__.__name__)

        self.polygon_connector = polygon_connector

    def build_ticker_mappings(self) -> dict:
        """
        Builds a collection of ticker mappings:

        1. All valid tickers
        2. CUSIP -> ticker
        3. CIK -> ticker
        4. ISIN -> ticker
        5. Country Composite FIGI -> ticker
        6. Global Share Class FIGI -> ticker
        """

        try:
            
            # fetch tickers
            tickers_data = self.polygon_connector.get_all_tickers()
            tickers = [td['ticker'] for td in tickers_data]
            assert(all([td['market'] == 'stocks' for td in tickers_data]))
            assert(all([td['type'] == 'CS' for td in tickers_data]))
            assert(all([td['active'] for td in tickers_data]))

            # fetch exchanges
            exchanges = self.polygon_connector.get_exchanges()
            exchange_mics = [ex['mic'] for ex in exchanges]





        except Exception as e:
            self.logger.exception('Error in build_mapping: ' + str(e))
            return None


    def build_exchange_mappings(self) -> dict:
        raise NotImplementedError

    def build_locale_mappings(self) -> dict:
        raise NotImplementedError