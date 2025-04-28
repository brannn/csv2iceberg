"""
Tests for the Iceberg catalog manager
"""
import pytest
import pyarrow as pa
from connectors.iceberg_catalog import IcebergCatalog

@pytest.fixture
def catalog():
    """Create a test catalog."""
    return IcebergCatalog(table_location="s3://test-bucket/test-table")

@pytest.fixture
def table():
    """Create a test table."""
    return pa.table({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"]
    })

def test_generate_manifest_path(catalog):
    """Test manifest path generation."""
    path = catalog._generate_manifest_path()
    assert path.startswith("s3://test-bucket/test-table/metadata/manifest-")
    assert path.endswith(".avro")

def test_generate_data_file_path(catalog):
    """Test data file path generation."""
    path = catalog._generate_data_file_path()
    assert path.startswith("s3://test-bucket/test-table/data/")
    assert path.endswith(".parquet")

def test_create_manifest(catalog, table):
    """Test manifest creation."""
    # Create a data file entry
    data_file = catalog.create_data_file(table)
    
    # Create a manifest
    manifest = catalog.create_manifest([data_file])
    
    # Verify the manifest
    assert manifest["manifest_path"].startswith("s3://test-bucket/test-table/metadata/manifest-")
    assert manifest["manifest_length"] == 0
    assert manifest["partition_spec_id"] == 0
    assert manifest["content"] == 0
    assert manifest["added_files_count"] == 1
    assert manifest["added_rows_count"] == 3

def test_create_snapshot(catalog, table):
    """Test snapshot creation."""
    # Create a data file entry
    data_file = catalog.create_data_file(table)
    
    # Create a manifest
    manifest = catalog.create_manifest([data_file])
    
    # Create a snapshot
    snapshot = catalog.create_snapshot(manifest)
    
    # Verify the snapshot
    assert snapshot["snapshot_id"] > 0
    assert snapshot["timestamp_ms"] > 0
    assert snapshot["manifest_list"] == manifest["manifest_path"]
    assert snapshot["summary"]["operation"] == "append"
    assert snapshot["summary"]["added-data-files"] == 1
    assert snapshot["summary"]["added-records"] == 3

def test_create_data_file(catalog, table):
    """Test data file creation."""
    # Create a data file entry
    data_file = catalog.create_data_file(table)
    
    # Verify the data file
    assert data_file["content"] == 0
    assert data_file["file_path"].startswith("s3://test-bucket/test-table/data/")
    assert data_file["file_format"] == "PARQUET"
    assert data_file["record_count"] == 3
    assert data_file["file_size_in_bytes"] == 0
    assert data_file["column_sizes"] == {}
    assert data_file["value_counts"] == {}
    assert data_file["null_value_counts"] == {}
    assert data_file["nan_value_counts"] == {}
    assert data_file["lower_bounds"] == {}
    assert data_file["upper_bounds"] == {}
    assert data_file["key_metadata"] is None
    assert data_file["split_offsets"] == []
    assert data_file["sort_order_id"] == 0

def test_create_commit(catalog, table):
    """Test commit creation."""
    # Create a data file entry
    data_file = catalog.create_data_file(table)
    
    # Create a manifest
    manifest = catalog.create_manifest([data_file])
    
    # Create a snapshot
    snapshot = catalog.create_snapshot(manifest)
    
    # Create a commit
    commit = catalog.create_commit(snapshot, manifest, [data_file])
    
    # Verify the commit
    assert commit["type"] == "append"
    assert commit["snapshot_id"] == snapshot["snapshot_id"]
    assert commit["manifest_list"] == snapshot["manifest_list"]
    assert len(commit["manifests"]) == 1
    assert commit["manifests"][0] == manifest
    assert len(commit["data_files"]) == 1
    assert commit["data_files"][0] == data_file 