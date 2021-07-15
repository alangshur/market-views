from datetime import datetime, timezone

from src.raw.sec13f import SEC13FLoader
from src.aws.s3 import AWSS3Connector
from src.api.polygon import PolygonAPIConnector
from src.api.sec import SECAPIConnector


s3_connector = AWSS3Connector(credentials_file_path='config/aws.json')
sec_connector = SECAPIConnector(credentials_file_path='config/sec.json')
polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')

polygon_connector.query_ticker_with_cusip('2824100')

# sec_connector.query_13f_filings(
#     fetch_from_dt=datetime(2021, 7, 14).astimezone(timezone.utc),
#     polygon_connector=polygon_connector
# )

# sec_13f_loader = SEC13FLoader(
#     s3_connector=s3_connector, 
#     polygon_connector=polygon_connector,
#     sec_connector=sec_connector,
#     manifest_s3_bucket_name='market-views-raw-manifest',
#     manifest_s3_object_name='sec-13f-manifest.json',
#     data_s3_bucket_name='market-views-sec-13f',
#     delay_time_secs=0,
#     fetch_from_override_dt=datetime(2015, 1, 1, tzinfo=timezone.utc)
# )

# filing_data = sec_13f_loader.update()
# monitor_metrics = sec_13f_loader.get_monitor_metrics()
# print(monitor_metrics)