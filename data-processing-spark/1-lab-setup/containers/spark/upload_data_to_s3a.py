import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

def create_s3_client(access_key, secret_key, endpoint, region):
    """
    Create a boto3 client configured for Minio or any S3-compatible service.

    :param access_key: S3 access key
    :param secret_key: S3 secret key
    :param endpoint: Endpoint URL for the S3 service
    :param region: Region to use, defaults to us-east-1
    :return: Configured S3 client
    """
    return boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )