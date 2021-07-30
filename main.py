from datetime import date, datetime, timezone

from src.raw.sec13f import SEC13FLoader
from src.aws.s3 import AWSS3Connector
from src.utils.mapping import MappingModule
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.sec import SECAPIConnector


# s3_connector = AWSS3Connector(credentials_file_path='config/aws.json')
# sec_connector = SECAPIConnector(credentials_file_path='config/sec.json')
# polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')


# sec_13f_loader = SEC13FLoader(
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

# url = 'https://www.sec.gov/Archives/edgar/data/1380106/000110465921094107/tm2122156-19_4seq1.xml'
# output = sec_connector._fetch_form_4_xml_data(url)
# print(output)

# filings = sec_connector.query_form_4_filings(datetime(2019, 1, 1, tzinfo=timezone.utc))
# print(filings)


polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')
raf_connector = RankAndFiledAPIConnector(credentials_file_path='config/raf.json')
mapping_module = MappingModule(polygon_connector, raf_connector)
multi_index = mapping_module.build_ticker_mappings()
print(multi_index)
