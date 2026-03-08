import boto3
import os

def get_r2_client(access_key, secret_key, account_id):
    return boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'
    )

def download_brief(key, bucket, access_key, secret_key, account_id):
    client = get_r2_client(access_key, secret_key, account_id)
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def upload_brief(data, key, bucket, access_key, secret_key, account_id):
    client = get_r2_client(access_key, secret_key, account_id)
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
