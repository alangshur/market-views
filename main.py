from src.utils.functional.identifiers import print_mapping_identifier_stats
from src.raw.sec13f import SEC13FLoader
from src.aws.s3 import AWSS3Connector
from src.utils.mapping import MappingModule
from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.sec import SECAPIConnector
from src.api.secgov import SECGovAPIConnector
from src.api.gleif import GLEIFAPIConnector


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


polygon_connector = PolygonAPIConnector(credentials_file_path='config/polygon.json')
raf_connector = RankAndFiledAPIConnector(credentials_file_path='config/raf.json')
sec_gov_connector = SECGovAPIConnector(credentials_file_path='config/secgov.json')
gleif_connector = GLEIFAPIConnector(credentials_file_path='config/gleif.json')

mapping_module = MappingModule(polygon_connector, raf_connector, sec_gov_connector, gleif_connector)
multi_index = mapping_module.build_ticker_mappings()
print_mapping_identifier_stats(multi_index)