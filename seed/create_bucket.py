import boto3
from src.config import settings


def ensure_bucket_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3.endpoint_url,
        aws_access_key_id=settings.s3.access_key,
        aws_secret_access_key=settings.s3.secret_key,
        use_ssl=settings.s3.secure,
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if settings.s3.bucket not in buckets:
        s3.create_bucket(Bucket=settings.s3.bucket)
        print(f"Bucket '{settings.s3.bucket}' created.")
    else:
        print(f"Bucket '{settings.s3.bucket}' already exists.")
