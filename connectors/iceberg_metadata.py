"""
Iceberg metadata manager for S3 Tables integration
"""
import logging
from typing import Dict, Any, List, Optional
import pyarrow as pa

logger = logging.getLogger(__name__)

class IcebergMetadata:
    """Manager for Iceberg metadata operations"""
    
    def __init__(self):
        """Initialize the Iceberg metadata manager."""
        logger.debug("Initialized Iceberg metadata manager")
    
    def convert_arrow_schema(self, schema: pa.Schema) -> Dict[str, Any]:
        """Convert a PyArrow schema to Iceberg schema.
        
        Args:
            schema: PyArrow schema
            
        Returns:
            Iceberg schema
        """
        fields = []
        
        for field in schema:
            # Convert PyArrow type to Iceberg type
            iceberg_type = self._convert_arrow_type(field.type)
            
            # Create field entry
            field_entry = {
                "id": len(fields),
                "name": field.name,
                "type": iceberg_type,
                "required": not field.nullable,
                "doc": field.metadata.get(b"description", b"").decode() if field.metadata else None
            }
            
            fields.append(field_entry)
        
        # Create schema entry
        schema_entry = {
            "type": "struct",
            "fields": fields,
            "schema-id": 0,
            "identifier-field-ids": []
        }
        
        logger.debug(f"Converted PyArrow schema to Iceberg schema: {schema_entry}")
        return schema_entry
    
    def _convert_arrow_type(self, arrow_type: pa.DataType) -> Dict[str, Any]:
        """Convert a PyArrow type to Iceberg type.
        
        Args:
            arrow_type: PyArrow data type
            
        Returns:
            Iceberg type
        """
        if isinstance(arrow_type, pa.BooleanType):
            return {"type": "boolean"}
            
        elif isinstance(arrow_type, pa.Int8Type):
            return {"type": "integer", "width": 8}
            
        elif isinstance(arrow_type, pa.Int16Type):
            return {"type": "integer", "width": 16}
            
        elif isinstance(arrow_type, pa.Int32Type):
            return {"type": "integer", "width": 32}
            
        elif isinstance(arrow_type, pa.Int64Type):
            return {"type": "integer", "width": 64}
            
        elif isinstance(arrow_type, pa.FloatType):
            return {"type": "float"}
            
        elif isinstance(arrow_type, pa.DoubleType):
            return {"type": "double"}
            
        elif isinstance(arrow_type, pa.StringType):
            return {"type": "string"}
            
        elif isinstance(arrow_type, pa.BinaryType):
            return {"type": "binary"}
            
        elif isinstance(arrow_type, pa.Date32Type):
            return {"type": "date"}
            
        elif isinstance(arrow_type, pa.TimestampType):
            return {"type": "timestamp", "unit": "micros"}
            
        elif isinstance(arrow_type, pa.DecimalType):
            return {
                "type": "decimal",
                "precision": arrow_type.precision,
                "scale": arrow_type.scale
            }
            
        elif isinstance(arrow_type, pa.ListType):
            return {
                "type": "list",
                "element-id": 0,  # Will be updated when converting full schema
                "element": self._convert_arrow_type(arrow_type.value_type),
                "element-required": not arrow_type.value_type.nullable
            }
            
        elif isinstance(arrow_type, pa.MapType):
            return {
                "type": "map",
                "key-id": 0,  # Will be updated when converting full schema
                "value-id": 1,  # Will be updated when converting full schema
                "key": self._convert_arrow_type(arrow_type.key_type),
                "value": self._convert_arrow_type(arrow_type.item_type),
                "value-required": not arrow_type.item_type.nullable
            }
            
        elif isinstance(arrow_type, pa.StructType):
            fields = []
            for i, field in enumerate(arrow_type):
                field_entry = {
                    "id": i,
                    "name": field.name,
                    "type": self._convert_arrow_type(field.type),
                    "required": not field.type.nullable,
                    "doc": field.metadata.get(b"description", b"").decode() if field.metadata else None
                }
                fields.append(field_entry)
            
            return {
                "type": "struct",
                "fields": fields
            }
            
        else:
            raise ValueError(f"Unsupported PyArrow type: {arrow_type}")
    
    def create_partition_spec(self, schema: Dict[str, Any], 
                            partition_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a partition specification.
        
        Args:
            schema: Iceberg schema
            partition_fields: List of partition field definitions
            
        Returns:
            Partition specification
        """
        # Create partition spec entry
        partition_spec = {
            "spec-id": 0,
            "fields": []
        }
        
        # Add partition fields
        for i, field in enumerate(partition_fields):
            # Find the field in the schema
            schema_field = next(
                (f for f in schema["fields"] if f["name"] == field["name"]),
                None
            )
            
            if not schema_field:
                raise ValueError(f"Partition field not found in schema: {field['name']}")
            
            # Create partition field entry
            partition_field = {
                "source-id": schema_field["id"],
                "field-id": 1000 + i,  # Use high field IDs for partition fields
                "name": field["name"],
                "transform": field["transform"]
            }
            
            partition_spec["fields"].append(partition_field)
        
        logger.debug(f"Created partition spec: {partition_spec}")
        return partition_spec
    
    def create_table_properties(self, properties: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Create table properties.
        
        Args:
            properties: Optional additional properties
            
        Returns:
            Table properties
        """
        # Start with default properties
        table_properties = {
            "format-version": "2",
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
            "write.parquet.row-group-size-bytes": "134217728",  # 128MB
            "write.parquet.page-size-bytes": "1048576",  # 1MB
            "write.parquet.dict-size-bytes": "2097152",  # 2MB
            "write.parquet.bloom-filter-enabled": "true",
            "write.parquet.bloom-filter-max-bytes": "1048576",  # 1MB
            "write.parquet.bloom-filter-ndv": "1000000"  # 1M
        }
        
        # Add custom properties
        if properties:
            table_properties.update(properties)
        
        logger.debug(f"Created table properties: {table_properties}")
        return table_properties 