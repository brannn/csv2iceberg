"""
Tests for the Iceberg REST client
"""
import pytest
import responses
from connectors.iceberg_rest_client import IcebergRestClient

@pytest.fixture
def client():
    """Create a test client."""
    return IcebergRestClient(
        endpoint="http://localhost:8080",
        region="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

@responses.activate
def test_create_table(client):
    """Test table creation."""
    # Mock the response
    responses.add(
        responses.POST,
        "http://localhost:8080/v1/tables",
        json={
            "name": "test.test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "type": "integer",
                        "required": True
                    }
                ]
            },
            "properties": {
                "format-version": "2"
            }
        },
        status=200
    )
    
    # Create a test schema
    schema = {
        "type": "struct",
        "fields": [
            {
                "id": 1,
                "name": "id",
                "type": "integer",
                "required": True
            }
        ]
    }
    
    # Create the table
    result = client.create_table(
        namespace="test",
        table_name="test_table",
        schema=schema
    )
    
    # Verify the result
    assert result["name"] == "test.test_table"
    assert result["schema"]["fields"][0]["name"] == "id"
    assert result["properties"]["format-version"] == "2"

@responses.activate
def test_list_tables(client):
    """Test listing tables."""
    # Mock the response
    responses.add(
        responses.GET,
        "http://localhost:8080/v1/namespaces/test/tables",
        json=["test_table1", "test_table2"],
        status=200
    )
    
    # List tables
    result = client.list_tables("test")
    
    # Verify the result
    assert len(result) == 2
    assert "test_table1" in result
    assert "test_table2" in result

@responses.activate
def test_get_table(client):
    """Test getting table metadata."""
    # Mock the response
    responses.add(
        responses.GET,
        "http://localhost:8080/v1/tables/test/test_table",
        json={
            "name": "test.test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "type": "integer",
                        "required": True
                    }
                ]
            },
            "properties": {
                "format-version": "2"
            }
        },
        status=200
    )
    
    # Get table metadata
    result = client.get_table("test", "test_table")
    
    # Verify the result
    assert result["name"] == "test.test_table"
    assert result["schema"]["fields"][0]["name"] == "id"
    assert result["properties"]["format-version"] == "2"

@responses.activate
def test_update_table(client):
    """Test updating table metadata."""
    # Mock the response
    responses.add(
        responses.POST,
        "http://localhost:8080/v1/tables/test/test_table/update",
        json={
            "name": "test.test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "type": "integer",
                        "required": True
                    },
                    {
                        "id": 2,
                        "name": "name",
                        "type": "string",
                        "required": True
                    }
                ]
            },
            "properties": {
                "format-version": "2"
            }
        },
        status=200
    )
    
    # Update table metadata
    updates = {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "type": "integer",
                    "required": True
                },
                {
                    "id": 2,
                    "name": "name",
                    "type": "string",
                    "required": True
                }
            ]
        }
    }
    
    result = client.update_table("test", "test_table", updates)
    
    # Verify the result
    assert result["name"] == "test.test_table"
    assert len(result["schema"]["fields"]) == 2
    assert result["schema"]["fields"][1]["name"] == "name"

@responses.activate
def test_delete_table(client):
    """Test deleting a table."""
    # Mock the response
    responses.add(
        responses.DELETE,
        "http://localhost:8080/v1/tables/test/test_table",
        status=204
    )
    
    # Delete the table
    client.delete_table("test", "test_table")
    
    # Verify the request was made
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == "http://localhost:8080/v1/tables/test/test_table"

@responses.activate
def test_commit(client):
    """Test committing changes."""
    # Mock the response
    responses.add(
        responses.POST,
        "http://localhost:8080/v1/tables/test/test_table/commit",
        json={
            "snapshot-id": 1,
            "manifest-list": "s3://test/manifests/manifest-1.avro"
        },
        status=200
    )
    
    # Create a test commit
    operations = [
        {
            "type": "append",
            "data-files": [
                {
                    "content": 0,
                    "file-path": "s3://test/data/1.parquet",
                    "file-format": "PARQUET",
                    "record-count": 100,
                    "file-size-in-bytes": 1000
                }
            ]
        }
    ]
    
    result = client.commit("test", "test_table", operations)
    
    # Verify the result
    assert result["snapshot-id"] == 1
    assert result["manifest-list"] == "s3://test/manifests/manifest-1.avro" 