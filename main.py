from datetime import datetime

from src.web.sec import SEC13FWebScraper
from src.utils.mapping import MappingModule

mapping_module = MappingModule()
scraper = SEC13FWebScraper(mapping_module)

since = datetime(2021, 4, 18)
filing_ids, next_query_dt = scraper.fetch_filing_ids(since)

print('Num filing ids: ' + str(len(filing_ids)))
print('Next query dt: ' + next_query_dt)
for id in filing_ids:
    print(id, flush=True)