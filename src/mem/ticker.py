import luhn

from src.utils.functional.identifiers import nn, convert_letters_to_string_numbers
from src.utils.logger import BaseModuleWithLogging
from src.utils.mindex import MultiIndex, MultiIndexException
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.secgov import SECGovAPIConnector
from src.api.gleif import GLEIFAPIConnector
from src.mem.base import BaseMemLoaderModule
from src.storage.redis import RedisStorageConnector


class TickerMemLoader(BaseMemLoaderModule):

    def __init__(self, redis_connector: RedisStorageConnector,
                 polygon_connector: PolygonAPIConnector, 
                 raf_connector: RankAndFiledAPIConnector,
                 sec_gov_connector: SECGovAPIConnector,
                 gleif_connector: GLEIFAPIConnector):

        super().__init__(self.__class__.__name__, redis_connector)

        self.polygon_connector = polygon_connector
        self.raf_connector = raf_connector
        self.sec_gov_connector = sec_gov_connector
        self.gleif_connector = gleif_connector

    def update(self) -> bool:
        try:
            self.logger.info('Starting update routine.')
        
            # fetch ticker data
            ticker_data = self.polygon_connector.get_internal_tickers()
            if ticker_data is None:
                raise Exception('missing ticker data')

            # fetch ticker details data
            ticker_details_data = self.polygon_connector.get_internal_ticker_details()
            if ticker_details_data is None:
                raise Exception('missing ticker details data')

            # fetch SEC cik data
            cik_data = self.sec_gov_connector.get_ciks()
            if cik_data is None: 
                raise Exception('missing cik data')

            # fetch SEC cusip data
            cusip_data = self.sec_gov_connector.get_cusips()
            if cusip_data is None:
                raise Exception('missing cusip data')

            # fetch RAF ticker data
            raf_ticker_data = self.raf_connector.get_tickers()
            if raf_ticker_data is None:
                raise Exception('missing RAF ticker data')

            # fetch RAF lei data
            raf_lei_data = self.raf_connector.get_leis()
            if raf_lei_data is None:
                raise Exception('missing RAF lei data')

            # fetch GLEIF lei data
            gleif_lei_data = self.gleif_connector.get_leis()
            if gleif_lei_data is None:
                raise Exception('missing GEIF lei data')

            # fetch exchange data
            exchange_data = self.polygon_connector.get_internal_exchanges()
            if exchange_data is None:
                raise Exception('missing exchange data')

            # fetch sector data
            industry_data = self.raf_connector.get_industries()
            if industry_data is None:
                raise Exception('missing industry data')

            # build multi-index
            self.logger.info('Building ticker mapping.')
            indices = ['ticker', 'name', 'cusip', 'cik', 'figi', 'isin', 'lei', 'bloomberg_gid', 'irs_number']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker_data_obj in ticker_data:
                try:
                    ticker = ticker_data_obj['ticker']
                    name = ticker_data_obj['name']

                    # get cusip
                    cusip_data_obj = cusip_data.get('ticker', ticker)
                    if nn(cusip_data_obj): cusip = cusip_data_obj['cusip']
                    else: cusip = None

                    # get cik
                    cik_data_obj = cik_data.get('ticker', ticker)
                    raf_ticker_data_obj = raf_ticker_data.get('ticker', ticker)
                    ticker_details_data_obj = ticker_details_data.get('ticker', ticker)
                    if nn(cik_data_obj): cik = cik_data_obj['cik']
                    elif nn(ticker_details_data_obj) and nn(ticker_details_data_obj['cik']): cik = ticker_details_data_obj['cik']
                    elif nn(raf_ticker_data_obj): cik = raf_ticker_data_obj['cik']
                    else: cik = None

                    # get figi
                    if nn(ticker_data_obj['figi']): figi = ticker_data_obj['figi']
                    elif nn(ticker_details_data_obj) and nn(ticker_details_data_obj['figi']): figi = ticker_details_data_obj['figi']
                    else: figi = None

                    # get isin
                    if cusip is not None:
                        country_code = ticker_data_obj['locale'].upper()
                        isin_checksum_value = convert_letters_to_string_numbers(country_code + cusip)
                        isin_checksum_digit = str(luhn.generate(isin_checksum_value))
                        isin = country_code + cusip + isin_checksum_digit
                    else:
                        isin = None

                    # get lei
                    raf_lei_data_obj = raf_lei_data.get('cik', cik)
                    gleif_lei_data_obj = gleif_lei_data.get('isin', isin)
                    if nn(gleif_lei_data_obj): lei = gleif_lei_data_obj['lei']
                    elif nn(ticker_details_data_obj) and nn(ticker_details_data_obj['lei']): lei = ticker_details_data_obj['lei']
                    elif nn(raf_lei_data_obj): lei = raf_lei_data_obj['lei']
                    else: lei = None

                    # get bloomberg global id
                    if nn(ticker_details_data_obj) and nn(ticker_details_data_obj['bloomberg']): bloomberg_gid = ticker_details_data_obj['bloomberg']
                    else: bloomberg_gid = None

                    # get IRS number
                    if nn(raf_ticker_data_obj): irs_number = raf_ticker_data_obj['irs_number']
                    else: irs_number = None

                    # build index
                    ticker_mapping = {}
                    ticker_mapping['ticker'] = ticker
                    ticker_mapping['name'] = name
                    if nn(cusip): ticker_mapping['cusip'] = cusip
                    if nn(cik): ticker_mapping['cik'] = cik
                    if nn(figi): ticker_mapping['figi'] = figi
                    if nn(isin): ticker_mapping['isin'] = isin
                    if nn(lei): ticker_mapping['lei'] = lei
                    if nn(bloomberg_gid): ticker_mapping['bloomberg_gid'] = bloomberg_gid
                    if nn(irs_number): ticker_mapping['irs_number'] = irs_number

                    # insert ticker data
                    ticker_mapping['locale'] = ticker_data_obj['locale']
                    ticker_mapping['asset_class'] = ticker_data_obj['asset_class']
                    ticker_mapping['currency_code'] = ticker_data_obj['currency_code']
                    ticker_mapping['last_updated'] = ticker_data_obj['last_updated']

                    # insert ticker details data
                    if nn(ticker_details_data_obj):
                        ticker_mapping['details'] = {}
                        ticker_mapping['details']['sector'] = ticker_details_data_obj['sector']
                        ticker_mapping['details']['list_date'] = ticker_details_data_obj['list_date']
                        ticker_mapping['details']['ceo'] = ticker_details_data_obj['ceo']
                        ticker_mapping['details']['phone'] = ticker_details_data_obj['phone']
                        ticker_mapping['details']['employees'] = ticker_details_data_obj['employees']
                        ticker_mapping['details']['url'] = ticker_details_data_obj['url']
                        ticker_mapping['details']['description'] = ticker_details_data_obj['description']
                        ticker_mapping['details']['address'] = ticker_details_data_obj['hq_address']
                        ticker_mapping['details']['state'] = ticker_details_data_obj['hq_state']
                        ticker_mapping['details']['country'] = ticker_details_data_obj['hq_country']
                    
                    # get exchange data
                    mic = ticker_data_obj['exchange_mic']
                    exchange_data_obj = exchange_data.get('mic', mic)
                    if nn(exchange_data_obj): ticker_mapping['exchange'] = exchange_data_obj
                    else: ticker_mapping['exchange'] = None

                    # get industry data
                    if nn(ticker_details_data_obj) and nn(ticker_details_data_obj['sic']): sic = ticker_details_data_obj['sic']
                    elif nn(raf_ticker_data_obj): sic = raf_ticker_data_obj['sic']
                    else: sic = None
                    industry_data_obj = industry_data.get('sic', sic)
                    if nn(industry_data_obj): ticker_mapping['industry'] = industry_data_obj

                    # insert mapping
                    multi_index.insert(ticker_mapping)
                    
                except MultiIndexException:
                    continue

            # save ticker data
            self.logger.info('Saving new ticker data.')
            save_result = self._save_data('tickers', multi_index)
            if not save_result:
                self.logger.error('Failed to save ticker data.')
                return False

            self.logger.info('Finishing update routine.')
            return True

        except Exception as e:
            self.logger.exception('Error in update: ' + str(e))
            return False