from datetime import datetime

from src.raw.scrapers.sec import SEC13FScraper
from src.raw.managers.sec import SEC13FManager 

sec_13f_scraper = SEC13FScraper()
sec_13f_manager = SEC13FManager(sec_13f_scraper)

filing_data = sec_13f_manager.update()
print(filing_data)