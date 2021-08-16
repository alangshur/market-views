from pymongo.collection import Collection
import pymongo

from src.storage.base import BaseStorageConnector


class DocumentDBStorageConnector(BaseStorageConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

        # get AWS credentials
        self.mongodb_url = self.storage_credentials['local_mongodb_url']
        self.database_name = self.storage_credentials['database_name']

        # connect to s3
        self.client = pymongo.MongoClient(self.mongodb_url)
        self.db = self.client[self.database_name]

    def read_documents(self, collection: str,
                       query: dict=None,
                       limit: int=None) -> list:

        try:
            collection: Collection = self.db[collection]
            if query is None: cursor = collection.find()
            else: cursor = collection.find(query)

            if limit is not None:
                cursor = cursor.limit(limit)

            return list(limit)

        except Exception as e:
            self.logger.exception('Exception in query_documents: {}.'.format(e))
            return None

    def write_document(self, collection: str, document: dict) -> bool:
        try:
            collection: Collection = self.db[collection]
            collection.insert_one(document)
            return True
        except Exception as e:
            self.logger.exception('Exception in write_document: {}.'.format(e))
            return False

    

