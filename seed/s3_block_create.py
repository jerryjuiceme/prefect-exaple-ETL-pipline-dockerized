from prefect_aws import AwsCredentials, S3Bucket
from src.config import settings
from pydantic import SecretStr
from prefect_aws.s3 import S3Bucket


def create_s3_block():
    aws_creds = AwsCredentials(
        aws_access_key_id=settings.s3.access_key,
        aws_secret_access_key=SecretStr(settings.s3.secret_key),
        region_name="eu-central-1",
    )
    aws_creds.save("s3-credentials", overwrite=True)

    # Load credentials
    loaded_creds = AwsCredentials.load("s3-credentials")

    s3_block = S3Bucket(
        bucket_name=settings.s3.bucket,
        credentials=loaded_creds,
        endpoint_url="http://localhost:9000",  # type: ignore
    )
    try:
        if S3Bucket.load(settings.s3.bucket) is not None:
            print("S3 block already exists")
            return
    except ValueError as e:
        s3_block.save(settings.s3.bucket)
        s3_client = s3_block.credentials.get_s3_client()
        # s3_client.create_bucket(Bucket=settings.s3.bucket)
        print("S3 bucket created")
        return
