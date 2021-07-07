from src.aws.s3 import AWSS3Connector
from src.api.polygon import APIPolygonConnector
from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.sec import SEC13FManager

s3_connector = AWSS3Connector()

polygon_connector = APIPolygonConnector(
    credentials_file_path='config/polygon.json'
)

sec_13f_scraper = SEC13FScraper()

sec_13f_manager = SEC13FManager(
    sec_13f_scraper=sec_13f_scraper, 
    s3_connector=s3_connector, 
    polygon_connector=polygon_connector,
    manifest_s3_bucket_name='market-views-raw-manifest',
    manifest_s3_object_name='sec-13f-manifest.json',
    data_s3_bucket_name='market-views-sec-13f',
    default_history_size_days=1,
    default_delay_time_secs=1
)

filing_data = sec_13f_manager.update()
monitor_metrics = sec_13f_manager.get_monitor_metrics()
print(monitor_metrics)