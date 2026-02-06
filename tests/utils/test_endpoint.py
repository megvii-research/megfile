from megfile.utils.endpoint import is_cloudflare_r2


def test_is_cloudflare_r2_with_none():
    """Test is_cloudflare_r2 returns False when endpoint_url is None."""
    assert is_cloudflare_r2(None) is False


def test_is_cloudflare_r2_with_valid_endpoint():
    """Test is_cloudflare_r2 returns True for valid Cloudflare R2 endpoints."""
    assert is_cloudflare_r2("https://account.r2.cloudflarestorage.com") is True
    assert is_cloudflare_r2("https://my-account.r2.cloudflarestorage.com") is True
    assert is_cloudflare_r2("http://test.r2.cloudflarestorage.com") is True


def test_is_cloudflare_r2_case_insensitive():
    """Test is_cloudflare_r2 is case insensitive."""
    assert is_cloudflare_r2("https://account.R2.CLOUDFLARESTORAGE.COM") is True
    assert is_cloudflare_r2("https://account.R2.CloudflareStorage.com") is True


def test_is_cloudflare_r2_with_invalid_endpoint():
    """Test is_cloudflare_r2 returns False for non-R2 endpoints."""
    assert is_cloudflare_r2("https://s3.amazonaws.com") is False
    assert is_cloudflare_r2("https://oss-cn-hangzhou.aliyuncs.com") is False
    assert is_cloudflare_r2("https://storage.googleapis.com") is False
    assert is_cloudflare_r2("https://example.com") is False


def test_is_cloudflare_r2_with_partial_match():
    """Test is_cloudflare_r2 returns False for partial matches."""
    assert is_cloudflare_r2("https://r2.cloudflarestorage.com.example.com") is False
    assert is_cloudflare_r2("https://cloudflarestorage.com") is False
    assert is_cloudflare_r2("r2.cloudflarestorage") is False


def test_is_cloudflare_r2_with_empty_string():
    """Test is_cloudflare_r2 returns False for empty string."""
    assert is_cloudflare_r2("") is False
