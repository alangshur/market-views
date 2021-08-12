from datetime import timezone, timedelta, datetime, date
from dateutil import parser
from typing import Any
from tqdm import tqdm
import pandas as pd
import requests
import time

from src.utils.functional.identifiers import check_ticker, to_string, parse_cik
from src.utils.functional.prices import parse_price
from src.utils.mindex import MultiIndex
from src.api.base import BaseAPIConnector


class PolygonAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

    def get_internal_exchanges(self) -> MultiIndex:
        """
            Returns a multi-index with the following fields for all 
            internal ticker types:

            - mic (index)
            - name
            - type
            - market
            - tape_id
        """
        
        try:
            self.logger.info('Loading get_internal_exchanges from cloud.')
            exchanges_data = self._query_endpoint(
                endpoint_name='exchanges',    
                alt_domain=self.api_credentials['api-domain-meta'],
                check_ok=False
            )

            # post-process data
            indices = ['mic']
            multi_index = MultiIndex(indices, default_index_key='mic', safe_mode=True)
            for exchange_data in exchanges_data:
                try:
                    multi_index.insert({
                        'mic': to_string(exchange_data['mic']),
                        'name': to_string(exchange_data['name']),
                        'type': exchange_data['type'],
                        'market': exchange_data['market'],
                        'tape_id': exchange_data['tape'],
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_internal_exchanges: ' + str(e))
            return None
    
    def get_internal_tickers(self,
                             max_iters: int=15) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - name
            - locale
            - asset_class
            - exchange_mic
            - currency_code
            - figi
            - last_updated
        """

        try:
            tickers_data = []

            # send initial request
            self.logger.info('Loading get_internal_tickers from cloud.')
            response = requests.get(
                url=self.api_domain + 'tickers', 
                params={
                    'apiKey': self.api_key,
                    'limit': 1000,
                    'active': True,
                    'type': 'CS',
                    'market': 'stocks'
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert json_response['status'] == 'OK', 'bad response status'
            assert json_response['count'] > 0, 'no data in response'
            assert len(json_response['next_url']) > 0, 'no next_url in response'
            tickers_data.extend(json_response['results'])
            next_url = json_response['next_url']

            # iteratively request cursor
            iters = 0
            while True:
            
                # send initial request
                response = requests.get(
                    url=next_url, 
                    headers={
                        'Authorization': 'Bearer ' + self.api_key
                    }
                )

                # verify response
                response.raise_for_status()
                json_response = response.json()
                assert json_response['status'] == 'OK', 'bad response status'
                assert json_response['count'] > 0, 'no data in response'
                tickers_data.extend(json_response['results'])

                # check next url
                if 'next_url' not in json_response: 
                    break
                elif iters >= max_iters: 
                    raise Exception('max iterations exceeded')
                else: 
                    next_url = json_response['next_url']
                    iters += 1

            # post-process data
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker_data in tickers_data:
                try:

                    # check ticker
                    if not check_ticker(ticker_data['ticker']):
                        continue

                    # insert ticker
                    multi_index.insert({
                        'ticker': to_string(ticker_data['ticker']),
                        'name': to_string(ticker_data['name']),
                        'figi': None if 'composite_figi' not in ticker_data else to_string(ticker_data['composite_figi']),
                        'locale': to_string(ticker_data['locale']).upper(),
                        'asset_class': 'stocks',
                        'exchange_mic': to_string(ticker_data['primary_exchange']),
                        'currency_code': to_string(ticker_data['currency_name']).upper(),
                        'last_updated': parser.parse(ticker_data['last_updated_utc']).astimezone(timezone.utc).isoformat(),
                    })
                except Exception:
                    continue
            
            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_internal_tickers: ' + str(e))
            return None
    
    def get_internal_ticker_quotes(self, internal_tickers: MultiIndex,
                                   no_cache: bool=False,
                                   cache_expiry_delta: timedelta=timedelta(days=30)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - last_updated
            - quote
            - minute_bar
            - day_bar
            - prev_day_bar
        """

        try: 

            # get internal tickers
            internal_tickers = internal_tickers.get_all_key_values('ticker')

            # query ticker quotes
            self.logger.info('Loading get_internal_ticker_quotes from cloud.')
            response = requests.get(
                url=self.api_credentials['api-domain-snapshot'] + 'tickers', 
                params={
                    'apiKey': self.api_key
                }
            )

            # verify response
            response.raise_for_status()
            json_response = response.json()
            assert json_response['status'] == 'DELAYED', 'bad response status'
            
            # check cache if empty response
            if json_response['count'] < len(internal_tickers) and not no_cache:
                cached_item = self._get_cache('get_internal_ticker_quotes', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_quotes from cache.')
                    return cached_item
                else:
                    raise Exception('no data in response')
            elif json_response['count'] == 0:
                raise Exception('no data in response')
            elif json_response['count'] < len(internal_tickers):
                raise Exception('missing data in response')

            # get ticker quotes data
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker_quote in json_response['tickers']:
                ticker = ticker_quote['ticker']
                last_updated = datetime.fromtimestamp(int(ticker_quote['updated']) / 1e9, tz=timezone.utc).isoformat()
                if ticker not in internal_tickers:
                    continue

                # get last quote
                try:
                    bid_price = parse_price(ticker_quote['lastQuote']['p'])
                    bid_size = int(ticker_quote['lastQuote']['s'])
                    ask_price = parse_price(ticker_quote['lastQuote']['P'])
                    ask_size = int(ticker_quote['lastQuote']['S'])
                    mid_price = parse_price((bid_price + ask_price) / 2)
                    weigted_mid_price = parse_price((bid_price * ask_size + ask_price * bid_size) / (ask_size + bid_size))
                    timestamp = datetime.fromtimestamp(int(ticker_quote['lastQuote']['t']) / 1e9, tz=timezone.utc).isoformat()
                    quote = {
                        'bid_price': bid_price,
                        'bid_size': bid_size,
                        'ask_price': ask_price,
                        'ask_size': ask_size,
                        'mid_price': mid_price,
                        'weighted_mid_price': weigted_mid_price,
                        'timestamp': timestamp
                    }
                except Exception:
                    continue

                # get minute bar
                try:
                    high = parse_price(ticker_quote['min']['h'])
                    low = parse_price(ticker_quote['min']['l'])
                    open = parse_price(ticker_quote['min']['o'])
                    close = parse_price(ticker_quote['min']['c'])
                    vwap = parse_price(ticker_quote['min']['vw'])
                    volume = int(ticker_quote['min']['v'])
                    minute_bar = {
                        'high': high,
                        'low': low,
                        'open': open,
                        'close': close,
                        'vwap': vwap,
                        'volume': volume
                    }
                except Exception:
                    minute_bar = None

                # get day bar
                try:
                    high = parse_price(ticker_quote['day']['h'])
                    low = parse_price(ticker_quote['day']['l'])
                    open = parse_price(ticker_quote['day']['o'])
                    close = parse_price(ticker_quote['day']['c'])
                    vwap = parse_price(ticker_quote['day']['vw'])
                    volume = int(ticker_quote['day']['v'])
                    absolute_change = round(float(weigted_mid_price - open), 6)
                    relative_change = round(float((weigted_mid_price - open) / open), 6)
                    day_bar = {
                        'high': high,
                        'low': low,
                        'open': open,
                        'close': close,
                        'vwap': vwap,
                        'volume': volume,
                        'absolute_change': absolute_change,
                        'relative_change': relative_change
                    }
                except Exception:
                    day_bar = None

                # get previous day bar
                try:
                    high = parse_price(ticker_quote['prevDay']['h'])
                    low = parse_price(ticker_quote['prevDay']['l'])
                    open = parse_price(ticker_quote['prevDay']['o'])
                    close = parse_price(ticker_quote['prevDay']['c'])
                    vwap = parse_price(ticker_quote['prevDay']['vw'])
                    volume = int(ticker_quote['prevDay']['v'])
                    absolute_change = round(float(close - open), 6)
                    relative_change = round(float((close - open) / open), 6)
                    prev_day_bar = {
                        'high': high,
                        'low': low,
                        'open': open,
                        'close': close,
                        'vwap': vwap,
                        'volume': volume,
                        'absolute_change': absolute_change,
                        'relative_change': relative_change
                    }
                except Exception:
                    prev_day_bar = None

                multi_index.insert({
                    'ticker': ticker,
                    'last_updated': last_updated,
                    'quote': quote,
                    'minute_bar': minute_bar,
                    'day_bar': day_bar,
                    'prev_day_bar': prev_day_bar
                })

            # cache item
            self._add_cache('get_internal_ticker_quotes', 'all', multi_index, 
                            expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_quotes: ' + str(e))
            return None

    def get_internal_historical_quotes(self, internal_tickers: MultiIndex,
                                       history_start_date: date=date(2000, 1, 1),
                                       progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
        """

        try: 

            # check cache
            past_historical_quotes = self._get_cache('get_internal_historical_quotes', 'all')

            # get internal tickers
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_historical_quotes from cloud.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:
                    
                    # get previous historical data
                    if past_historical_quotes is not None:
                        cache_data = past_historical_quotes.get('ticker', ticker)
                        if cache_data is not None: past_historical_quotes_df = cache_data['historical_quotes']
                        else: past_historical_quotes_df = None
                    else:
                        cache_data = None
                        past_historical_quotes_df = None

                    # get start/end date strings
                    if past_historical_quotes_df is None: start_date = str(history_start_date)
                    else: start_date = past_historical_quotes_df.index[-1]
                    if start_date == date.today(): raise Exception
                    end_date = str(date.today() + timedelta(days=10))

                    # query historical quotes
                    historical_quotes = self._query_endpoint(
                        endpoint_name='{}/range/1/day/{}/{}'.format(ticker, start_date, end_date),
                        alt_domain=self.api_credentials['api-domain-aggs'],
                        delayed_status=True
                    )

                    # validate result
                    if historical_quotes is None or len(historical_quotes) == 0:
                        raise Exception

                    # parse quotes
                    historical_quotes_df = pd.DataFrame.from_records(historical_quotes)
                    historical_quotes_df.columns = ['volume', 'vwap', 'open', 'close', 'high', 'low', 'date', 'transactions']
                    historical_quotes_df['date'] = pd.to_datetime(historical_quotes_df['date'], unit='ms').dt.date
                    historical_quotes_df = historical_quotes_df.set_index('date', drop=True).sort_index(ascending=True)
                    historical_quotes_df = historical_quotes_df.groupby(historical_quotes_df.index).first()                    

                    # merge quotes
                    if past_historical_quotes_df is not None:
                        historical_quotes_df = pd.concat([past_historical_quotes_df, historical_quotes_df], axis=0)
                        historical_quotes_df = historical_quotes_df.sort_index(ascending=True)
                        historical_quotes_df = historical_quotes_df.groupby(historical_quotes_df.index).last()

                    # insert indices
                    multi_index.insert({
                        'ticker': ticker,
                        'historical_quotes': historical_quotes_df
                    })

                except Exception:
                    if cache_data is not None:
                        multi_index.insert(cache_data)
                
            # cache item
            self._add_cache('get_internal_historical_quotes', 'all', multi_index)
            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_historical_quotes: ' + str(e))
            return None

    def get_internal_ticker_details(self, internal_tickers: MultiIndex,
                                    no_cache: bool=False,
                                    cache_expiry_delta: timedelta=timedelta(days=7),
                                    progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - name
            - cik
            - figi
            - lei
            - bloomberg
            - sic
            - sector
            - industry
            - country
            - list_date
            - ceo
            - phone
            - employees
            - url
            - description
            - hq_address
            - hq_state
            - hq_country
        """

        try: 

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_internal_ticker_details', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_details from cache.')
                    return cached_item

            # get internal tickers
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_ticker_details from cloud.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:

                    # query ticker details
                    ticker_details = self._query_endpoint(
                        endpoint_name='symbols/{}/company'.format(ticker),    
                        alt_domain=self.api_credentials['api-domain-meta'],
                        check_ok=False
                    )

                    # insert indices
                    multi_index.insert({
                        'ticker': to_string(ticker_details['symbol']),
                        'name': to_string(ticker_details['name']),
                        'cik': parse_cik(to_string(ticker_details['cik'])),
                        'figi': to_string(ticker_details['figi']),
                        'lei': to_string(ticker_details['lei']),
                        'bloomberg': to_string(ticker_details['bloomberg']),
                        'sic': to_string(ticker_details['sic']),
                        'sector': ticker_details['sector'],
                        'industry': ticker_details['industry'],
                        'country': ticker_details['country'].upper(),
                        'list_date': ticker_details['listdate'],
                        'ceo': ticker_details['ceo'],
                        'phone': ticker_details['phone'],
                        'employees': ticker_details['employees'],
                        'url': ticker_details['url'],
                        'description': ticker_details['description'],
                        'hq_address': ticker_details['hq_address'],
                        'hq_state': ticker_details['hq_state'],
                        'hq_country': ticker_details['hq_country']
                    })

                except Exception:
                    pass
                
            # cache item
            self._add_cache('get_internal_ticker_details', 'all', multi_index, 
                            expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_details: ' + str(e))
            return None

    def get_internal_ticker_financials(self, internal_tickers: MultiIndex,
                                       no_cache: bool=False,
                                       cache_expiry_delta: timedelta=timedelta(days=7),
                                       progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
            - name
            - last_quarterly_report
            - last_annual_report
        """

        try: 

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_internal_ticker_financials', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_financials from cache.')
                    return cached_item

            # get internal tickers
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_ticker_financials from cloud.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:

                    # query quarterly financial details
                    quarterly_financials = self._query_endpoint(
                        endpoint_name='financials', 
                        alt_domain=self.api_credentials['api-domain-vx'],
                        additional_params={
                            'ticker': ticker,
                            'limit': 1,
                            'timeframe': 'quarterly'
                        }
                    )

                    # parse quarterly details
                    fiscal_period = str(quarterly_financials[0]['fiscal_period']) + ' ' + str(quarterly_financials[0]['fiscal_year'])
                    quarterly_details = {'fiscal_period': fiscal_period}
                    quarterly_financials = quarterly_financials[0]['financials']
                    if quarterly_financials is None or len(quarterly_financials) == 0: continue
                    for financial_class in quarterly_financials.keys():
                        quarterly_details[financial_class] = {}
                        for key, value in quarterly_financials[financial_class].items():
                            quarterly_details[financial_class][key] = value['value']
                        if len(quarterly_details[financial_class]) == 0:
                            quarterly_details[financial_class] = None

                    # query annual financial details
                    annual_financials = self._query_endpoint(
                        endpoint_name='financials', 
                        alt_domain=self.api_credentials['api-domain-vx'],
                        additional_params={
                            'ticker': ticker,
                            'limit': 1,
                            'timeframe': 'annual'
                        }
                    )

                    # parse annual details
                    fiscal_year = str(annual_financials[0]['fiscal_year'])
                    annual_details = {'fiscal_year': fiscal_year}
                    annual_financials = annual_financials[0]['financials']
                    if annual_financials is None or len(annual_financials) == 0: continue
                    for financial_class in annual_financials.keys():
                        annual_details[financial_class] = {}
                        for key, value in annual_financials[financial_class].items():
                            annual_details[financial_class][key] = value['value']
                        if len(annual_details[financial_class]) == 0:
                            annual_details[financial_class] = None

                    # insert indices
                    multi_index.insert({
                        'ticker': ticker,
                        'last_quarterly_report': quarterly_details,
                        'last_annual_report': annual_details
                    })

                except Exception:
                    pass
                
            # cache item
            self._add_cache('get_internal_ticker_financials', 'all', multi_index, 
                            expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_financials: ' + str(e))
            return None

    def get_internal_ticker_dividends(self, internal_tickers: MultiIndex, ticker_quotes: MultiIndex,
                                      no_cache: bool=False,
                                      cache_expiry_delta: timedelta=timedelta(days=7),
                                      progress_bar: bool=False) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            internal tickers:

            - ticker (index)
        """

        try: 

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_internal_ticker_dividends', 'all')
                if cached_item is not None: 
                    self.logger.info('Loading get_internal_ticker_dividends from cache.')
                    return cached_item

            # get internal tickers
            internal_tickers = internal_tickers.get_all_key_values('ticker')
                
            # get ticker details data
            self.logger.info('Loading get_internal_ticker_dividends from cloud.')
            indices = ['ticker']
            multi_index = MultiIndex(indices, default_index_key='ticker', safe_mode=True)
            for ticker in tqdm(internal_tickers, disable=not progress_bar):
                try:

                    # query ticker details
                    ticker_dividends = self._query_endpoint(
                        endpoint_name='dividends/{}'.format(ticker),    
                        alt_domain=self.api_credentials['api-domain-v2']
                    )

                    # get ticker quote
                    quote = ticker_quotes.get('ticker', ticker)
                    if quote is None: continue
                    else: price = quote['quote']['weighted_mid_price']
                    
                    # get last year dividends
                    idx = 0
                    annual_dividends = []
                    cur_dividend_date = ticker_dividends[idx]['paymentDate']
                    cur_dividend_date = parser.parse(cur_dividend_date).date()
                    start_dividend_date = cur_dividend_date - timedelta(days=400)
                    while cur_dividend_date > start_dividend_date:
                        annual_dividends.append(ticker_dividends[idx]['amount'])
                        idx += 1
                        cur_dividend_date = ticker_dividends[idx]['paymentDate']
                        cur_dividend_date = parser.parse(cur_dividend_date).date()

                    # get dividend metrics
                    if len(annual_dividends) > 6:
                        dividend_type = 'monthly'
                        annual_dividends = annual_dividends[:12]
                        rolling_annual_dividend = float(sum(annual_dividends))
                        annual_dividend = float(annual_dividends[0] * 12)
                        dividend_yield = round(float(annual_dividend / price), 6)
                    else:
                        dividend_type = 'quarterly'
                        annual_dividends = annual_dividends[:4]
                        rolling_annual_dividend = float(sum(annual_dividends))
                        annual_dividend = float(annual_dividends[0] * 4)
                        dividend_yield = round(float(annual_dividend / price), 6)

                    multi_index.insert({
                        'ticker': ticker,
                        'dividend_type': dividend_type,
                        'rolling_annual_dividend': rolling_annual_dividend,
                        'annual_dividend': annual_dividend,
                        'dividend_yield': dividend_yield,
                        'last_dividend': {
                            'amount': float(ticker_dividends[0]['amount']),
                            'ex_date': parser.parse(ticker_dividends[0]['exDate']).astimezone(timezone.utc).isoformat(),
                            'payment_date': parser.parse(ticker_dividends[0]['paymentDate']).astimezone(timezone.utc).isoformat(),
                            'record_date': parser.parse(ticker_dividends[0]['recordDate']).astimezone(timezone.utc).isoformat()
                        }
                    })

                except Exception:
                    pass
                
            # cache item
            self._add_cache('get_internal_ticker_dividends', 'all', multi_index, 
                            expiry_delta=cache_expiry_delta)

            return multi_index
            
        except Exception as e:
            self.logger.exception('Error in get_internal_ticker_dividends: ' + str(e))
            return None

    def _query_endpoint(self, endpoint_name: str,
                        alt_domain: str=None,
                        check_ok: bool=True,
                        additional_params: dict={},
                        delayed_status=False) -> Any: 

        # send no-param request
        if alt_domain is None: domain = self.api_domain
        else: domain = alt_domain
        response = requests.get(
            url=domain + endpoint_name, 
            params={
                'apiKey': self.api_key,
                **additional_params
            }
        )

        # verify response
        response.raise_for_status()
        json_response = response.json()
        if check_ok:
            if delayed_status: assert json_response['status'] == 'DELAYED', 'bad response status'
            else: assert json_response['status'] == 'OK', 'bad response status'
            return json_response['results']
        else:
            return json_response