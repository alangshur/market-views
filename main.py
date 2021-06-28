from datetime import datetime

from src.utils.mapping import MappingModule
from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.sec import SEC13FManager 

mapping_module = MappingModule()
sec_13f_scraper = SEC13FScraper()
sec_13f_manager = SEC13FManager(mapping_module, sec_13f_scraper)

filing_data = sec_13f_manager.update()
print(filing_data)