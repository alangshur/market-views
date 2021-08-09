from datetime import timedelta
from io import BytesIO
import requests
import zipfile

from src.utils.functional.identifiers import to_string
from src.api.base import BaseAPIConnector
from src.utils.mindex import MultiIndex


class GLEIFAPIConnector(BaseAPIConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

    def get_leis(self,
                 no_cache: bool=False,
                 cache_expiry_delta: timedelta=timedelta(days=1)) -> MultiIndex:

        """
            Returns a multi-index with the following fields for all 
            legal ticker types from the GLEIF database:

            - isin (index)
            - lei
        """

        try:

            # check cache
            if not no_cache:
                cached_item = self._get_cache('get_leis', 'all')
                if cached_item is not None:
                    self.logger.info('Loading get_leis from cache.')
                    return cached_item
            
            # get leis files data
            self.logger.info('Loading get_leis from cloud.')
            response = requests.get(self.api_domain)
            response.raise_for_status()
            data_files = response.json()
            download_url = data_files['data'][0]['links']['download']
            download_url = download_url.replace('\/', '/')

            # get leis data
            response = requests.get(download_url)
            response.raise_for_status()

            # parse ZIP file
            fp = BytesIO(response.content)
            zip_fp = zipfile.ZipFile(fp, 'r')
            data_fp = zip_fp.open(zip_fp.namelist()[0])
            lines = data_fp.read().decode('utf-8').splitlines()
            data = [line.split(',') for line in lines[1:]]
            data = [row for row in data if row[1].startswith('US')]
            zip_fp.close()
            
            # build multi-index
            indices = ['isin']
            multi_index = MultiIndex(indices, default_index_key='isin', safe_mode=True)
            for row in data:
                try:
                    multi_index.insert({
                        'isin': to_string(row[1]),
                        'lei': to_string(row[0])
                    })
                except Exception:
                    continue
            
            # cache item
            if not no_cache:
                self._add_cache('get_leis', 'all', multi_index, 
                                expiry_delta=cache_expiry_delta)

            return multi_index

        except Exception as e:
            self.logger.exception('Error in get_leis: ' + str(e))
            return None