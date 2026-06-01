import boto3


def make_moto_s3_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
