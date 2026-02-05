import boto3
import pytest
from moto import mock_aws

from megfile.s3_path import _s3_fast_list_objects_recursive


@pytest.fixture
def s3_empty_client(mocker):
    """Create an empty S3 client with moto"""
    with mock_aws():
        client = boto3.client("s3")
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)

        # Use mocker.spy to track list_objects_v2 calls
        mocker.spy(client, "list_objects_v2")

        yield client


def create_files(s3_client, bucket, file_keys):
    """Helper to create multiple files in S3"""
    s3_client.create_bucket(Bucket=bucket)
    for key in file_keys:
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"test content")


def collect_all_keys(responses):
    """Collect all keys from list_objects_v2 responses"""
    keys = []
    for resp in responses:
        for content in resp.get("Contents", []):
            keys.append(content["Key"])
    return sorted(keys)


def test_fast_list_no_truncation(s3_empty_client):
    """Test when first batch has no truncation - should return immediately"""
    bucket = "test-bucket"
    files = ["prefix/file1.txt", "prefix/file2.txt"]
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(results) == 1  # Single response, no pagination
    assert s3_empty_client.list_objects_v2.call_count == 1


def test_fast_list_dense_directory_serial(s3_empty_client):
    """Test Strategy 1: All files at current level - use serial listing"""
    bucket = "test-bucket"
    # Create 50 files at current directory level
    files = [f"prefix/file{i:04d}.txt" for i in range(50)]
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(results) == 1  # All 50 files in one response
    assert s3_empty_client.list_objects_v2.call_count == 1


def test_fast_list_single_subdir_serial(s3_empty_client):
    """Test Strategy 2a: Only one subdir - continue serial"""
    bucket = "test-bucket"
    # Create files in a single subdirectory
    files = [f"prefix/subdir/file{i:04d}.txt" for i in range(50)]
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(results) == 1  # Single subdir should use serial listing
    assert s3_empty_client.list_objects_v2.call_count == 1


def test_fast_list_multiple_subdirs_parallel(s3_empty_client):
    """Test Strategy: Multiple subdirs with even distribution - use serial"""
    bucket = "test-bucket"
    # Create 1200 files in 5 subdirectories (240 files each) to trigger truncation
    files = []
    for dir_num in range(5):
        for file_num in range(240):
            files.append(f"prefix/dir{dir_num}/file{file_num:04d}.txt")
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(keys) == 1200
    # First 1000 files span multiple subdirs (even distribution)
    # Algorithm should use serial continuation: ~2 requests
    assert len(results) == 2  # Serial continuation is more efficient
    # Strategy 2b: First batch spans multiple subdirs, so no get_all_subdirs call
    # Total: 2 list_objects_v2 calls (first batch + continuation)
    assert s3_empty_client.list_objects_v2.call_count == 2


def test_fast_list_nested_subdirs_recursive(s3_empty_client):
    """Test that nested subdirs are also processed with adaptive strategy"""
    bucket = "test-bucket"
    files = []
    # Create many files in nested structure to trigger truncation
    # 300 files in dir0/nested0, 300 in dir0/nested1, 300 in dir1,
    # 300 in dir2 = 1200 files
    for i in range(300):
        files.append(f"prefix/dir0/nested0/file{i:04d}.txt")
        files.append(f"prefix/dir0/nested1/file{i:04d}.txt")
        files.append(f"prefix/dir1/file{i:04d}.txt")
        files.append(f"prefix/dir2/file{i:04d}.txt")
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(keys) == 1200
    # First 1000 files span 3 top-level subdirs (dir0, dir1, dir2) - even distribution
    # Should use serial at top level: 2 responses
    assert len(results) == 2
    # 2 list_objects_v2 calls for serial continuation at root level
    assert s3_empty_client.list_objects_v2.call_count == 2


def test_fast_list_many_subdirs_parallel(s3_empty_client):
    """Test Strategy: Many subdirs with even distribution - use serial"""
    bucket = "test-bucket"
    # Create files in 20 subdirectories, 60 files each = 1200 files
    files = []
    for dir_num in range(20):
        for file_num in range(60):
            files.append(f"prefix/dir{dir_num:02d}/file{file_num}.txt")
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(keys) == 1200  # 20 dirs * 60 files
    # First 1000 files span many subdirs (even distribution)
    # Should use serial - ~2 requests
    assert len(results) == 2
    # 2 list_objects_v2 calls (first batch + continuation)
    assert s3_empty_client.list_objects_v2.call_count == 2


def test_fast_list_uneven_distribution_parallel(s3_empty_client):
    """Test Strategy 2c: Uneven distribution - use parallel"""
    bucket = "test-bucket"
    files = []
    # Create 1200 files concentrated in dir0, but have 10 total subdirs
    for file_num in range(1200):
        files.append(f"prefix/dir0/file{file_num:04d}.txt")
    # Add a few files in other subdirs to create them
    for dir_num in range(1, 10):
        files.append(f"prefix/dir{dir_num}/file0.txt")
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(keys) == 1209
    # First 1000 files all in dir0 (concentrated), but 10 subdirs total
    # Should use parallel listing by subdir
    # dir0 needs 2 responses (1000+200), other 9 dirs need 1 each = 11 total
    assert len(results) == 11
    # Strategy 2c: First batch + get_all_subdirs (with continuation)
    # + 10 subdir listings = 1 (first batch) + ~1 (get_all_subdirs)
    # + 11 (subdir responses) = 13 calls
    assert s3_empty_client.list_objects_v2.call_count == 13


def test_fast_list_empty_prefix(s3_empty_client):
    """Test with empty prefix"""
    bucket = "test-bucket"
    files = ["file1.txt", "file2.txt", "dir1/file3.txt"]
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, ""))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(results) == 1
    assert s3_empty_client.list_objects_v2.call_count == 1


def test_fast_list_mixed_structure(s3_empty_client):
    """Test with mixed structure: files at root and in subdirs"""
    bucket = "test-bucket"
    files = []
    # Files in subdirs - create enough to trigger truncation (400 per dir)
    for i in range(400):
        files.append(f"prefix/dir1/file{i:04d}.txt")
        files.append(f"prefix/dir2/file{i:04d}.txt")
        files.append(f"prefix/dir3/file{i:04d}.txt")
    # Also add some files at root level
    files.extend(["prefix/file0.txt", "prefix/file1.txt"])
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    # First 1000 files span multiple subdirs (even distribution)
    # Should use serial continuation: keeps all files including root-level
    assert len(keys) == 1202
    assert "prefix/file0.txt" in keys
    assert "prefix/file1.txt" in keys
    assert len(results) == 2  # Serial continuation
    # 2 list_objects_v2 calls (first batch + continuation)
    assert s3_empty_client.list_objects_v2.call_count == 2


def test_fast_list_deep_nesting(s3_empty_client):
    """Test with deeply nested directory structure"""
    bucket = "test-bucket"
    files = [
        "prefix/a/b/c/d/e/file1.txt",
        "prefix/a/b/c/d/e/file2.txt",
        "prefix/a/b/c/file3.txt",
        "prefix/a/file4.txt",
    ]
    create_files(s3_empty_client, bucket, files)

    results = list(_s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/"))
    keys = collect_all_keys(results)

    assert keys == sorted(files)
    assert len(results) == 1
    assert s3_empty_client.list_objects_v2.call_count == 1


def test_fast_list_correctness_comparison(s3_empty_client):
    """Compare results with standard list_objects to ensure correctness"""
    bucket = "test-bucket"
    # Create a complex structure
    files = []
    for i in range(10):
        files.append(f"prefix/root_file{i}.txt")
        for j in range(3):
            files.append(f"prefix/dir{i}/file{j}.txt")
            files.append(f"prefix/dir{i}/subdir{j}/file{j}.txt")

    create_files(s3_empty_client, bucket, files)

    # Get results from fast recursive method
    fast_results = list(
        _s3_fast_list_objects_recursive(s3_empty_client, bucket, "prefix/")
    )
    fast_keys = collect_all_keys(fast_results)

    # Get results from standard method (paginate through all)
    standard_keys = []
    paginator = s3_empty_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="prefix/"):
        for content in page.get("Contents", []):
            standard_keys.append(content["Key"])

    # Results should be identical (order doesn't matter)
    assert sorted(fast_keys) == sorted(standard_keys)
    assert len(fast_keys) == len(files)
