from src.api.secgov import SECGovAPIConnector
from src.mem.base import BaseMemLoaderModule
from src.storage.redis import RedisStorageConnector


class CIKLookupMemLoader(BaseMemLoaderModule):

    def __init__(self, redis_connector: RedisStorageConnector,
                 sec_gov_connector: SECGovAPIConnector):

        super().__init__(self.__class__.__name__, redis_connector)
        self.sec_gov_connector = sec_gov_connector

    def update(self) -> bool:
        try:
            self.logger.info('Starting update routine.')

            # fetch financials
            multi_index = self.sec_gov_connector.get_all_ciks()

            # save ticker data
            self.logger.info('Saving new CIK lookup data.')
            save_result = self._save_data('cik_lookup', multi_index)
            if not save_result:
                self.logger.error('Failed to save CIK lookup data.')
                return False

            self.logger.info('Finishing update routine.')
            return True

        except Exception as e:
            self.logger.exception('Error in update: ' + str(e))
            return False