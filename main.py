from datetime import datetime

from src.aws.s3 import AWSS3Connector

from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.sec import SEC13FManager 

aws_s3_connector = AWSS3Connector()

sec_13f_manifest_s3_bucket_name = 'market-views-raw-manifest'
sec_13f_manifest_s3_object_name = 'sec-13f-manifest.json'

sec_13f_scraper = SEC13FScraper()
sec_13f_manager = SEC13FManager(
    sec_13f_scraper=sec_13f_scraper, 
    aws_s3_connector=aws_s3_connector, 
    manifest_s3_bucket_name=sec_13f_manifest_s3_bucket_name,
    manifest_s3_object_name=sec_13f_manifest_s3_object_name
)

filing_data = sec_13f_manager.update()
print(filing_data)