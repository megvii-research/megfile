import boto3


def make_moto_s3_client(monkeypatch=None):
    if monkeypatch is not None:
        monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
