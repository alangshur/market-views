import boto3
import botocore
import json
import io

from src.aws.base import AWSBaseConnector


class AWSS3Connector(AWSBaseConnector):

    def __init__(self,
                 raw_manifest_bucket_name: str='market-views-raw-manifest'):

        super().__init__(self.__class__.__name__)

        # connect to s3
        self.s3_resource = boto3.resource('s3', 
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region_name
        )

        self.raw_manifest_bucket_name = raw_manifest_bucket_name

    def get_raw_manifest(self, manifest_name: str) -> dict:
        try:

            # fetch manifest from s3
            try: 
                s3_manifest_object = self.s3_resource.Object(self.raw_manifest_bucket_name, manifest_name)
                aws_response = s3_manifest_object.get()
            except botocore.errorfactory.NoSuchKey: 
                return None

            # parse s3 response
            response_code = int(aws_response['ResponseMetadata']['HTTPStatusCode'])
            if response_code != 200:
                raise Exception('received {} response from AWS'.format(response_code))
            else:

                # deserialize manifest
                serialized_manifest = aws_response['Body'].read().decode('utf-8')
                manifest = json.loads(serialized_manifest)
                return manifest

        except Exception as e:
            self.logger.exception('Exception in get_raw_manifest: {}.'.format(e))
            return None
        
    def save_raw_manifest(self, manifest_name: str, manifest: dict) -> bool:
        try:

            # serialize manifest dict
            serialized_manifest = json.dumps(manifest).encode('utf-8')
            serialized_manifest = io.BytesIO(serialized_manifest)

            # save serialized manifest to s3
            s3_manifest_object = self.s3_resource.Object(self.raw_manifest_bucket_name, manifest_name)
            aws_response = s3_manifest_object.put(
                Body=serialized_manifest
            )

            # parse s3 response
            response_code = int(aws_response['ResponseMetadata']['HTTPStatusCode'])
            if response_code != 200:
                raise Exception('received {} response from AWS'.format(response_code))

            return True

        except Exception as e:
            self.logger.exception('Exception in save_raw_manifest: {}.'.format(e))
            return False
        

    

