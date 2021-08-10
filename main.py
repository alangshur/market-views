from src.utils.functional.identifiers import print_mapping_identifier_stats
from src.data.sec13f import SEC13FDataLoader
from src.storage.s3 import S3StorageConnector
from src.storage.redis import RedisStorageConnector
from src.mem.ticker import TickerMemLoader
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.sec import SECAPIConnector
from src.api.secgov import SECGovAPIConnector
from src.api.gleif import GLEIFAPIConnector


# s3_connector = S3StorageConnector(credentials_file_path='config/aws.json')
# sec_connector = SECAPIConnector(credentials_file_path='config/sec.json')
# polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')


# sec_13f_loader = SEC13FDataLoader(
#     s3_connector=s3_connector, 
#     polygon_connector=polygon_connector,
#     sec_connector=sec_connector,
#     manifest_s3_bucket_name='market-views-raw-manifest',
#     manifest_s3_object_name='sec-13f-manifest.json',
#     data_s3_bucket_name='market-views-sec-13f',
#     delay_time_secs=0,
#     fetch_from_override_dt=datetime(2021, 1, 1, tzinfo=timezone.utc)
# )


# filing_data = sec_13f_loader.update()
# monitor_metrics = sec_13f_loader.get_monitor_metrics()
# print(monitor_metrics)


# redis_connector = RedisStorageConnector(credentials_file_path='config/redis.json')

polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')
# raf_connector = RankAndFiledAPIConnector(credentials_file_path='config/raf.json')
# sec_gov_connector = SECGovAPIConnector(credentials_file_path='config/secgov.json')
# gleif_connector = GLEIFAPIConnector(credentials_file_path='config/gleif.json')

# ticker_mem_loader = TickerMemLoader(redis_connector, polygon_connector, raf_connector, sec_gov_connector, gleif_connector)
# ticker_mem_loader.update()

print(polygon_connector.get_internal_ticker_quotes())


# TODO: 
# - add last quote API (mem loader)
# - add historical data API (data loader)
# - add historical splits (mem loader)
# - add historical dividends (mem loader)
# - add cik to ticker/name endpoint
# - add current dividend data to ticker mapping