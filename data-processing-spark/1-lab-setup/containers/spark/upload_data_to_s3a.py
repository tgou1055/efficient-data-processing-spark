import os
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

def upload_data_to_s3(s3_client, data_directory, bucket_name):
    """
    Upload a data directory to S3A (Minio)

    :param s3_client: S3 client object
    :param data_directory: data directory (absolute)
    :param bucket_name: bucket to upload
    :return:
    """
    try:
        for root, dirs, files in os.walk(data_directory):
            for file in files:
                # Construct the full file path
                local_file_path = os.path.join(root, file)
                # Construct the S3 object key (relative path inside the bucket)
                s3_key = os.path.relpath(local_file_path, data_directory)
                # Upload the file
                s3_client.upload_file(local_file_path, bucket_name, s3_key)
                print(f"uploaded {local_file_path} to {bucket_name}/{s3_key}")
    except ClientError as error:
        print(f"Fail to upload files: {error}")


# Credentials and Connection Info
access_key = 'minio'
secret_key = 'minio123'
endpoint = 'http://minio:9000'
region = 'us-east-1'

# Client upload directory to s3a
try:
    s3_client = create_s3_client(access_key, secret_key, endpoint, region)
    data_directory = '/opt/spark/work-dir/data/'
    bucket_name = 'data'
    upload_data_to_s3(s3_client, data_directory, bucket_name)

except:
    print("Full catch, check bucket creation script at create_buckets.py")