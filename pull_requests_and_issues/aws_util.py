import ray
from pathlib import Path
import boto3


def list_parquet_files_s3(bucket_name, path, aws_access_key_id, aws_secret_access_key):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    res = []
    for obj in bucket.objects.filter(Prefix=path):
        if obj.key.endswith('parquet'):
            res.append(obj.key)
    return res


@ray.remote
def download_s3(bucket_name, keys, dst, aws_access_key_id, aws_secret_access_key):
    dst = Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for key in keys:
        key = Path(key)
        bucket.download_file(
            str(key),
            str(dst / key.name)
        )