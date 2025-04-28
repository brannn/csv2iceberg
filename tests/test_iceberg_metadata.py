"""
Tests for the Iceberg metadata manager
"""
import pytest
import pyarrow as pa
from connectors.iceberg_metadata import IcebergMetadata

@pytest.fixture
def metadata():
    """Create a test metadata manager."""
    return IcebergMetadata()

@pytest.fixture
def schema():
    """Create a test schema."""
    return pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("age", pa.float64(), nullable=True),
        pa.field("active", pa.bool_(), nullable=False),
        pa.field("created_at", pa.timestamp("us"), nullable=False),
        pa.field("balance", pa.decimal128(10, 2), nullable=True),
        pa.field("tags", pa.list_(pa.string()), nullable=True),
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True)
    ])

def test_convert_arrow_schema(metadata, schema):
    """Test schema conversion."""
    # Convert the schema
    result = metadata.convert_arrow_schema(schema)
    
    # Verify the result
    assert result["type"] == "struct"
    assert len(result["fields"]) == 8
    assert result["schema-id"] == 0
    assert result["identifier-field-ids"] == []
    
    # Verify field types
    field_types = {f["name"]: f["type"]["type"] for f in result["fields"]}
    assert field_types["id"] == "integer"
    assert field_types["name"] == "string"
    assert field_types["age"] == "double"
    assert field_types["active"] == "boolean"
    assert field_types["created_at"] == "timestamp"
    assert field_types["balance"] == "decimal"
    assert field_types["tags"] == "list"
    assert field_types["metadata"] == "map"
    
    # Verify field properties
    field_props = {f["name"]: f["required"] for f in result["fields"]}
    assert field_props["id"] is True
    assert field_props["name"] is False
    assert field_props["age"] is False
    assert field_props["active"] is True
    assert field_props["created_at"] is True
    assert field_props["balance"] is False
    assert field_props["tags"] is False
    assert field_props["metadata"] is False

def test_create_partition_spec(metadata, schema):
    """Test partition spec creation."""
    # Convert the schema
    iceberg_schema = metadata.convert_arrow_schema(schema)
    
    # Create partition fields
    partition_fields = [
        {"name": "id", "transform": "identity"},
        {"name": "created_at", "transform": "day"}
    ]
    
    # Create partition spec
    result = metadata.create_partition_spec(iceberg_schema, partition_fields)
    
    # Verify the result
    assert result["spec-id"] == 0
    assert len(result["fields"]) == 2
    
    # Verify partition fields
    field_names = [f["name"] for f in result["fields"]]
    assert "id" in field_names
    assert "created_at" in field_names
    
    # Verify transforms
    field_transforms = {f["name"]: f["transform"] for f in result["fields"]}
    assert field_transforms["id"] == "identity"
    assert field_transforms["created_at"] == "day"

def test_create_table_properties(metadata):
    """Test table properties creation."""
    # Create table properties
    result = metadata.create_table_properties()
    
    # Verify the result
    assert result["format-version"] == "2"
    assert result["write.format.default"] == "parquet"
    assert result["write.parquet.compression-codec"] == "snappy"
    assert result["write.parquet.row-group-size-bytes"] == "134217728"
    assert result["write.parquet.page-size-bytes"] == "1048576"
    assert result["write.parquet.dict-size-bytes"] == "2097152"
    assert result["write.parquet.bloom-filter-enabled"] == "true"
    assert result["write.parquet.bloom-filter-max-bytes"] == "1048576"
    assert result["write.parquet.bloom-filter-ndv"] == "1000000"

def test_create_table_properties_custom(metadata):
    """Test table properties creation with custom properties."""
    # Create custom properties
    custom_props = {
        "write.parquet.compression-codec": "gzip",
        "write.parquet.row-group-size-bytes": "268435456"
    }
    
    # Create table properties
    result = metadata.create_table_properties(custom_props)
    
    # Verify the result
    assert result["format-version"] == "2"
    assert result["write.format.default"] == "parquet"
    assert result["write.parquet.compression-codec"] == "gzip"
    assert result["write.parquet.row-group-size-bytes"] == "268435456"
    assert result["write.parquet.page-size-bytes"] == "1048576"
    assert result["write.parquet.dict-size-bytes"] == "2097152"
    assert result["write.parquet.bloom-filter-enabled"] == "true"
    assert result["write.parquet.bloom-filter-max-bytes"] == "1048576"
    assert result["write.parquet.bloom-filter-ndv"] == "1000000" 