import luhn

from src.utils.functional.identifiers import nn, convert_letters_to_string_numbers
from src.utils.logger import BaseModuleWithLogging
from src.utils.mindex import MultiIndex
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.secgov import SECGovAPIConnector
from src.api.gleif import GLEIFAPIConnector


class MappingModule(BaseModuleWithLogging):

    def __init__(self, polygon_connector: PolygonAPIConnector, 
                 raf_connector: RankAndFiledAPIConnector,
                 sec_gov_connector: SECGovAPIConnector,
                 gleif_connector: GLEIFAPIConnector):

        super().__init__(self.__class__.__name__)

        self.polygon_connector = polygon_connector
        self.raf_connector = raf_connector
        self.sec_gov_connector = sec_gov_connector
        self.gleif_connector = gleif_connector

    def build_ticker_mappings(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            internal ticker types:

            - ticker (index)
            - name (index)
            - cusip (index)
            - cik (index)
            - figi (index)
            - isin (index)
            - lei (index)
            - bloomberg_gid (index)
            - irs_number (index)
        """

        indices = [
            'ticker', 'name', 'cusip', 'cik', 
            'figi', 'isin', 'lei', 'bloomberg_gid', 'irs_number'
        ]

        try:
            self.logger.info('Building ticker mappings.')
        
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

            # build multi-index
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

                    # insert ticker details
                    ticker_mapping['locale'] = ticker_data_obj['locale']
                    ticker_mapping['asset_class'] = ticker_data_obj['asset_class']
                    ticker_mapping['currency_code'] = ticker_data_obj['currency_code']
                    ticker_mapping['last_updated'] = ticker_data_obj['last_updated']


                    
                    multi_index.insert(ticker_mapping)
                    
                except Exception:
                    continue

            return multi_index

        except Exception as e:
            self.logger.exception('Error in build_ticker_mappings: ' + str(e))
            return None


    def build_exchange_mappings(self) -> dict:
        raise NotImplementedError

    def build_locale_mappings(self) -> dict:
        raise NotImplementedError