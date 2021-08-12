from src.api.polygon import PolygonAPIConnector
from src.mem.base import BaseMemLoaderModule
from src.storage.redis import RedisStorageConnector


class TickerFinancialsMemLoader(BaseMemLoaderModule):

    def __init__(self, redis_connector: RedisStorageConnector,
                 polygon_connector: PolygonAPIConnector):

        super().__init__(self.__class__.__name__, redis_connector)
        self.polygon_connector = polygon_connector

    def update(self) -> bool:
        try:
            self.logger.info('Starting update routine.')

            # fetch financials
            internal_tickers = self.polygon_connector.get_internal_tickers()
            if internal_tickers is None:
                raise Exception('failed to retrieve internal tickers')
            else:
                multi_index = self.polygon_connector.get_internal_ticker_financials(internal_tickers)

            # save ticker data
            self.logger.info('Saving new ticker financials data.')
            save_result = self._save_data('ticker_financials', multi_index)
            if not save_result:
                self.logger.error('Failed to save ticker financials data.')
                return False

            self.logger.info('Finishing update routine.')
            return True

        except Exception as e:
            self.logger.exception('Error in update: ' + str(e))
            return False