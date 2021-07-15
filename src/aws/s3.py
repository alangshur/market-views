from botocore.exceptions import ClientError
import boto3
import json
import io

from src.aws.base import BaseAWSConnector


class AWSS3Connector(BaseAWSConnector):

    def __init__(self, credentials_file_path: str):
        super().__init__(self.__class__.__name__, credentials_file_path)

        # connect to s3
        self.s3_resource = boto3.resource('s3', 
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region_name
        )

    def read_json(self, bucket_name: str, object_name: str) -> dict:
        try:

            # fetch object from s3
            try: 
                s3_object = self.s3_resource.Object(bucket_name, object_name)
                aws_response = s3_object.get()
            except ClientError:
                return None

            # parse s3 response
            response_code = int(aws_response['ResponseMetadata']['HTTPStatusCode'])
            if response_code != 200:
                raise Exception('received {} response from AWS'.format(response_code))
            else:

                # deserialize manifest
                serialized_object = aws_response['Body'].read().decode('utf-8')
                object = json.loads(serialized_object)
                return object

        except Exception as e:
            self.logger.exception('Exception in get_json: {}.'.format(e))
            return None
        
    def write_json(self, bucket_name: str, object_name: str, object: dict) -> bool:
        try:

            # serialize manifest dict
            serialized_object = json.dumps(object).encode('utf-8')
            serialized_object = io.BytesIO(serialized_object)

            # save serialized manifest to s3
            s3_object = self.s3_resource.Object(bucket_name, object_name)
            aws_response = s3_object.put(
                Body=serialized_object
            )

            # parse s3 response
            response_code = int(aws_response['ResponseMetadata']['HTTPStatusCode'])
            if response_code != 200:
                raise Exception('received {} response from AWS'.format(response_code))

            return True

        except Exception as e:
            self.logger.exception('Exception in save_object: {}.'.format(e))
            return False
        

    

