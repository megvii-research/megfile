def is_cloudflare_r2(endpoint_url: str | None) -> bool:
    """Check if the endpoint is Cloudflare R2.

    :param endpoint_url: The endpoint URL to check
    :type endpoint_url: str | None
    :return: True if the endpoint is Cloudflare R2, False otherwise
    :rtype: bool
    """
    if endpoint_url is None:
        return False
    # https://developers.cloudflare.com/r2/api/s3/api/
    return endpoint_url.lower().endswith(".r2.cloudflarestorage.com")
