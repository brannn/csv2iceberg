"""
Tests for the S3 Parquet writer
"""
import pytest
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_s3
from connectors.s3_parquet_writer import S3ParquetWriter

@pytest.fixture
def writer():
    """Create a test writer."""
    return S3ParquetWriter(
        region="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

@pytest.fixture
def table():
    """Create a test table."""
    return pa.table({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"]
    })

@mock_s3
def test_write_dataframe(writer, table):
    """Test writing a dataframe to S3."""
    # Create a mock S3 bucket
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    
    # Write the table
    result = writer.write_dataframe(
        df=table,
        bucket="test-bucket",
        key="test.parquet"
    )
    
    # Verify the result
    assert result == "s3://test-bucket/test.parquet"
    
    # Verify the file was written
    response = s3.get_object(Bucket="test-bucket", Key="test.parquet")
    assert response["ContentType"] == "application/octet-stream"
    
    # Read the file back
    with pa.BufferReader(response["Body"].read()) as reader:
        result_table = pq.read_table(reader)
        
        # Verify the data
        assert result_table.num_rows == 3
        assert result_table.num_columns == 2
        assert result_table.column_names == ["id", "name"]

@mock_s3
def test_write_dataframe_multipart(writer, table):
    """Test writing a large dataframe using multipart upload."""
    # Create a mock S3 bucket
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    
    # Create a large table
    large_table = pa.table({
        "id": list(range(1000000)),
        "name": ["x"] * 1000000
    })
    
    # Write the table
    result = writer.write_dataframe_multipart(
        df=large_table,
        bucket="test-bucket",
        key="large.parquet",
        chunk_size=100000
    )
    
    # Verify the result
    assert result == "s3://test-bucket/large.parquet"
    
    # Verify the file was written
    response = s3.get_object(Bucket="test-bucket", Key="large.parquet")
    assert response["ContentType"] == "application/octet-stream"
    
    # Read the file back
    with pa.BufferReader(response["Body"].read()) as reader:
        result_table = pq.read_table(reader)
        
        # Verify the data
        assert result_table.num_rows == 1000000
        assert result_table.num_columns == 2
        assert result_table.column_names == ["id", "name"]

@mock_s3
def test_delete_file(writer):
    """Test deleting a file from S3."""
    # Create a mock S3 bucket
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    
    # Upload a test file
    s3.put_object(
        Bucket="test-bucket",
        Key="test.parquet",
        Body=b"test"
    )
    
    # Delete the file
    writer.delete_file("test-bucket", "test.parquet")
    
    # Verify the file was deleted
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket="test-bucket", Key="test.parquet")

@mock_s3
def test_list_files(writer):
    """Test listing files in S3."""
    # Create a mock S3 bucket
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    
    # Upload test files
    s3.put_object(
        Bucket="test-bucket",
        Key="test1.parquet",
        Body=b"test1"
    )
    s3.put_object(
        Bucket="test-bucket",
        Key="test2.parquet",
        Body=b"test2"
    )
    
    # List files
    result = writer.list_files("test-bucket", "test")
    
    # Verify the result
    assert len(result) == 2
    assert "s3://test-bucket/test1.parquet" in result
    assert "s3://test-bucket/test2.parquet" in result

@mock_s3
def test_file_exists(writer):
    """Test checking if a file exists in S3."""
    # Create a mock S3 bucket
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    
    # Upload a test file
    s3.put_object(
        Bucket="test-bucket",
        Key="test.parquet",
        Body=b"test"
    )
    
    # Check if the file exists
    assert writer.file_exists("test-bucket", "test.parquet") is True
    assert writer.file_exists("test-bucket", "nonexistent.parquet") is False 