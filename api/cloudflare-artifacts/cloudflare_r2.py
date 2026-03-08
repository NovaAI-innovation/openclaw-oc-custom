import boto3
import json
from botocore.client import Config

def get_r2_client(access_key, secret_key, account_id):
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4")
    )

def upload_brief(brief_dict, key, bucket_name, access_key, secret_key, account_id):
    client = get_r2_client(access_key, secret_key, account_id)
    json_data = json.dumps(brief_dict)
    client.put_object(Bucket=bucket_name, Key=key, Body=json_data, ContentType="application/json")
    print(f"Uploaded brief to {key} in {bucket_name}")

def download_brief(key, bucket_name, access_key, secret_key, account_id):
    client = get_r2_client(access_key, secret_key, account_id)
    response = client.get_object(Bucket=bucket_name, Key=key)
    data = response["Body"].read().decode("utf-8")
    return json.loads(data)
