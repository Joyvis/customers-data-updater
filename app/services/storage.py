import boto3

from app.config import settings


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )


def upload_file(file_content: bytes, key: str) -> str:
    """Upload bytes to S3/MinIO under the configured bucket. Returns the key."""
    client = _get_s3_client()
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=file_content,
    )
    return key


def download_file(key: str) -> bytes:
    """Download file content from S3/MinIO by key. Returns raw bytes."""
    client = _get_s3_client()
    response = client.get_object(Bucket=settings.s3_bucket, Key=key)
    return response["Body"].read()


def generate_presigned_url(key: str, expires_in: int = 3600) -> str:
    """Generate a presigned download URL for the given S3/MinIO key."""
    client = _get_s3_client()
    url: str = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.s3_bucket, "Key": key},
        ExpiresIn=expires_in,
    )
    return url
