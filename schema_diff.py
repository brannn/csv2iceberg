"""
Utility for comparing Iceberg schemas and detecting changes.
"""
import logging
from typing import Dict, List, Optional, Set, Tuple, Any, NamedTuple

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, Type, StructType, 
    StringType, BinaryType, 
    IntegerType, LongType, FloatType, DoubleType, DecimalType,
    BooleanType, DateType, TimeType, TimestampType
)

from models import CompatibilityLevel, ChangeType, SchemaVersion, SchemaChange

logger = logging.getLogger(__name__)

class FieldChange(NamedTuple):
    """Represents a detected change in a schema field."""
    field_name: str
    change_type: ChangeType
    prev_field: Optional[NestedField] = None
    new_field: Optional[NestedField] = None
    compatibility: CompatibilityLevel = CompatibilityLevel.FULL
    description: Optional[str] = None

class SchemaDiff:
    """
    Utility for comparing two Iceberg schemas and identifying changes.
    """
    
    # Type compatibility mapping
    # Each entry defines what types can be safely converted to what other types
    # Key is source type, value is dict of target types with compatibility levels
    TYPE_COMPATIBILITY = {
        # Integer type compatibility
        IntegerType: {
            IntegerType: CompatibilityLevel.FULL,
            LongType: CompatibilityLevel.FULL,
            FloatType: CompatibilityLevel.FULL,
            DoubleType: CompatibilityLevel.FULL,
            DecimalType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            # Incompatible: DateType, TimestampType, BooleanType, BinaryType
        },
        LongType: {
            LongType: CompatibilityLevel.FULL,
            FloatType: CompatibilityLevel.FULL,
            DoubleType: CompatibilityLevel.FULL,
            DecimalType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            IntegerType: CompatibilityLevel.BACKWARD,  # Potential data loss
            # Incompatible: DateType, TimestampType, BooleanType, BinaryType
        },
        # Float type compatibility
        FloatType: {
            FloatType: CompatibilityLevel.FULL,
            DoubleType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            IntegerType: CompatibilityLevel.BACKWARD,  # Potential data loss
            LongType: CompatibilityLevel.BACKWARD,  # Potential data loss
            # Incompatible: DecimalType, DateType, TimestampType, BooleanType, BinaryType
        },
        DoubleType: {
            DoubleType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            FloatType: CompatibilityLevel.BACKWARD,  # Potential data loss
            # Incompatible: IntegerType, LongType, DecimalType, DateType, TimestampType, BooleanType, BinaryType
        },
        # Decimal type compatibility
        DecimalType: {
            DecimalType: CompatibilityLevel.FULL,  # Note: Precision/scale should be checked separately
            StringType: CompatibilityLevel.FULL,
            # Incompatible: IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, BooleanType, BinaryType
        },
        # String type compatibility
        StringType: {
            StringType: CompatibilityLevel.FULL,
            # All types can be converted to string, but string to other types requires checking
            # Backward compatible only because the data may not parse as the target type
            IntegerType: CompatibilityLevel.BACKWARD,
            LongType: CompatibilityLevel.BACKWARD,
            FloatType: CompatibilityLevel.BACKWARD,
            DoubleType: CompatibilityLevel.BACKWARD,
            DecimalType: CompatibilityLevel.BACKWARD,
            DateType: CompatibilityLevel.BACKWARD,
            TimestampType: CompatibilityLevel.BACKWARD,
            BooleanType: CompatibilityLevel.BACKWARD,
            # Incompatible: BinaryType
        },
        # Date/Time type compatibility
        DateType: {
            DateType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            TimestampType: CompatibilityLevel.FULL,
            # Incompatible: IntegerType, LongType, FloatType, DoubleType, DecimalType, BooleanType, BinaryType
        },
        TimestampType: {
            TimestampType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            DateType: CompatibilityLevel.BACKWARD,  # Time information is lost
            # Incompatible: IntegerType, LongType, FloatType, DoubleType, DecimalType, BooleanType, BinaryType
        },
        # Boolean type compatibility
        BooleanType: {
            BooleanType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            # Incompatible: IntegerType, LongType, FloatType, DoubleType, DecimalType, DateType, TimestampType, BinaryType
        },
        # Binary type compatibility
        BinaryType: {
            BinaryType: CompatibilityLevel.FULL,
            StringType: CompatibilityLevel.FULL,
            # Incompatible: IntegerType, LongType, FloatType, DoubleType, DecimalType, DateType, TimestampType, BooleanType
        }
    }
    
    @staticmethod
    def compare_schemas(
        prev_schema: Schema, 
        new_schema: Schema
    ) -> Tuple[List[FieldChange], CompatibilityLevel]:
        """
        Compare two schemas and return a list of field changes.
        
        Args:
            prev_schema: Previous schema
            new_schema: New schema
            
        Returns:
            A tuple containing:
                - List of field changes
                - Overall compatibility level
        """
        changes = []
        
        # Create lookup maps for prev and new fields by name
        prev_fields = {field.name: field for field in prev_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        # Check for fields removed from previous schema
        for name, field in prev_fields.items():
            if name not in new_fields:
                changes.append(FieldChange(
                    field_name=name,
                    change_type=ChangeType.REMOVE_COLUMN,
                    prev_field=field,
                    new_field=None,
                    compatibility=CompatibilityLevel.BACKWARD,  # Can read old data, but not write new data
                    description=f"Column '{name}' was removed"
                ))
        
        # Check for fields added or modified in new schema
        for name, new_field in new_fields.items():
            if name not in prev_fields:
                # New field added
                compatibility = CompatibilityLevel.FORWARD
                if not new_field.required:
                    # New nullable fields are fully compatible
                    compatibility = CompatibilityLevel.FULL
                
                changes.append(FieldChange(
                    field_name=name,
                    change_type=ChangeType.ADD_COLUMN,
                    prev_field=None,
                    new_field=new_field,
                    compatibility=compatibility,
                    description=f"Column '{name}' was added"
                ))
            else:
                # Field exists in both schemas, check for type or nullability changes
                prev_field = prev_fields[name]
                changes.extend(SchemaDiff._check_field_changes(prev_field, new_field))
        
        # Determine overall compatibility
        overall_compatibility = SchemaDiff._determine_overall_compatibility(changes)
        
        return changes, overall_compatibility
    
    @staticmethod
    def _check_field_changes(prev_field: NestedField, new_field: NestedField) -> List[FieldChange]:
        """
        Check for changes between two versions of the same field.
        
        Args:
            prev_field: Previous field
            new_field: New field
            
        Returns:
            List of detected changes
        """
        changes = []
        
        # Check for type changes
        if type(prev_field.field_type) != type(new_field.field_type):
            # Type has changed, determine compatibility
            compatibility = SchemaDiff._check_type_compatibility(
                prev_field.field_type, 
                new_field.field_type
            )
            
            changes.append(FieldChange(
                field_name=new_field.name,
                change_type=ChangeType.CHANGE_TYPE,
                prev_field=prev_field,
                new_field=new_field,
                compatibility=compatibility,
                description=f"Column '{new_field.name}' type changed from {prev_field.field_type} to {new_field.field_type}"
            ))
        
        # Check for nullability changes
        if prev_field.required != new_field.required:
            if prev_field.required and not new_field.required:
                # Making nullable is always compatible
                changes.append(FieldChange(
                    field_name=new_field.name,
                    change_type=ChangeType.CHANGE_NULLABILITY,
                    prev_field=prev_field,
                    new_field=new_field,
                    compatibility=CompatibilityLevel.FULL,
                    description=f"Column '{new_field.name}' changed from required to nullable"
                ))
            else:
                # Making required is backward compatible only
                changes.append(FieldChange(
                    field_name=new_field.name,
                    change_type=ChangeType.CHANGE_NULLABILITY,
                    prev_field=prev_field,
                    new_field=new_field,
                    compatibility=CompatibilityLevel.BACKWARD,
                    description=f"Column '{new_field.name}' changed from nullable to required"
                ))
        
        # For numeric types, check precision/scale changes
        if (isinstance(prev_field.field_type, DecimalType) and 
            isinstance(new_field.field_type, DecimalType)):
            changes.extend(SchemaDiff._check_decimal_changes(prev_field, new_field))
        
        # For struct types, recursively check nested fields
        if (isinstance(prev_field.field_type, StructType) and 
            isinstance(new_field.field_type, StructType)):
            # TODO: Implement recursive struct field comparison if needed
            pass
        
        return changes
    
    @staticmethod
    def _check_decimal_changes(prev_field: NestedField, new_field: NestedField) -> List[FieldChange]:
        """
        Check for changes in decimal precision/scale.
        
        Args:
            prev_field: Previous decimal field
            new_field: New decimal field
            
        Returns:
            List of detected changes
        """
        changes = []
        
        prev_decimal = prev_field.field_type
        new_decimal = new_field.field_type
        
        # Check precision changes
        if prev_decimal.precision != new_decimal.precision:
            if prev_decimal.precision < new_decimal.precision:
                # Increasing precision is compatible
                changes.append(FieldChange(
                    field_name=new_field.name,
                    change_type=ChangeType.CHANGE_TYPE,
                    prev_field=prev_field,
                    new_field=new_field,
                    compatibility=CompatibilityLevel.FULL,
                    description=f"Column '{new_field.name}' precision increased from {prev_decimal.precision} to {new_decimal.precision}"
                ))
            else:
                # Decreasing precision is potentially lossy
                changes.append(FieldChange(
                    field_name=new_field.name,
                    change_type=ChangeType.CHANGE_TYPE,
                    prev_field=prev_field,
                    new_field=new_field,
                    compatibility=CompatibilityLevel.BACKWARD,
                    description=f"Column '{new_field.name}' precision decreased from {prev_decimal.precision} to {new_decimal.precision}"
                ))
        
        # Check scale changes
        if prev_decimal.scale != new_decimal.scale:
            changes.append(FieldChange(
                field_name=new_field.name,
                change_type=ChangeType.CHANGE_TYPE,
                prev_field=prev_field,
                new_field=new_field,
                compatibility=CompatibilityLevel.BACKWARD,
                description=f"Column '{new_field.name}' scale changed from {prev_decimal.scale} to {new_decimal.scale}"
            ))
        
        return changes
    
    @staticmethod
    def _check_type_compatibility(
        prev_type: Type, 
        new_type: Type
    ) -> CompatibilityLevel:
        """
        Check compatibility between two field types.
        
        Args:
            prev_type: Previous type
            new_type: New type
            
        Returns:
            Compatibility level
        """
        # Get the compatibility mapping for the previous type
        prev_type_class = type(prev_type)
        compatibility_map = SchemaDiff.TYPE_COMPATIBILITY.get(prev_type_class, {})
        
        # Check compatibility with the new type
        new_type_class = type(new_type)
        compatibility = compatibility_map.get(new_type_class, CompatibilityLevel.NONE)
        
        return compatibility
    
    @staticmethod
    def _determine_overall_compatibility(changes: List[FieldChange]) -> CompatibilityLevel:
        """
        Determine the overall compatibility level from a list of changes.
        
        Args:
            changes: List of field changes
            
        Returns:
            Overall compatibility level
        """
        if not changes:
            return CompatibilityLevel.FULL
        
        # Start with the most permissive level
        overall = CompatibilityLevel.FULL
        
        # Reduce to the most restrictive level
        for change in changes:
            if change.compatibility == CompatibilityLevel.NONE:
                # One incompatible change makes the whole schema incompatible
                return CompatibilityLevel.NONE
            elif change.compatibility == CompatibilityLevel.BACKWARD and overall == CompatibilityLevel.FORWARD:
                # Can't be both backward and forward compatible without being fully compatible
                overall = CompatibilityLevel.NONE
            elif change.compatibility == CompatibilityLevel.FORWARD and overall == CompatibilityLevel.BACKWARD:
                # Can't be both backward and forward compatible without being fully compatible
                overall = CompatibilityLevel.NONE
            elif change.compatibility.value > overall.value:
                # Move to a more restrictive level
                overall = change.compatibility
        
        return overall
    
    @staticmethod
    def record_schema_changes(
        catalog: str,
        schema: str,
        table: str,
        new_schema: Schema,
        created_by: Optional[str] = None,
        change_description: Optional[str] = None
    ) -> SchemaVersion:
        """
        Compare a new schema with the latest version and record the changes.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            new_schema: New schema
            created_by: User who created this version
            change_description: Description of the changes
            
        Returns:
            New SchemaVersion object
        """
        # Get the latest schema version
        latest_version = SchemaVersion.get_latest_version(catalog, schema, table)
        
        # If this is the first version, just record it
        if latest_version is None:
            return SchemaVersion.create_from_iceberg_schema(
                catalog=catalog,
                schema=schema,
                table=table,
                iceberg_schema=new_schema,
                created_by=created_by,
                change_description="Initial schema version",
                compatibility_level=CompatibilityLevel.FULL
            )
        
        # Convert the stored schema definition to a PyIceberg Schema
        prev_schema = SchemaDiff._json_to_iceberg_schema(latest_version.schema_definition)
        
        # Compare the schemas
        changes, overall_compatibility = SchemaDiff.compare_schemas(prev_schema, new_schema)
        
        # Create a new schema version
        new_version = SchemaVersion.create_from_iceberg_schema(
            catalog=catalog,
            schema=schema,
            table=table,
            iceberg_schema=new_schema,
            created_by=created_by,
            change_description=change_description,
            compatibility_level=overall_compatibility
        )
        
        # Record individual changes
        for change in changes:
            SchemaChange.create_change(
                schema_version_id=new_version.id,
                field_name=change.field_name,
                change_type=change.change_type,
                compatibility=change.compatibility,
                prev_type=str(change.prev_field.field_type) if change.prev_field else None,
                new_type=str(change.new_field.field_type) if change.new_field else None,
                change_description=change.description
            )
        
        return new_version
    
    @staticmethod
    def _json_to_iceberg_schema(schema_json: Dict[str, Any]) -> Schema:
        """
        Convert a JSON schema representation back to a PyIceberg Schema object.
        
        This is a simplified implementation that assumes the schema was
        created using SchemaVersion._iceberg_schema_to_json
        
        Args:
            schema_json: JSON schema representation
            
        Returns:
            PyIceberg Schema object
        """
        from pyiceberg.schema import Schema
        from pyiceberg.types import Type, NestedField, StructType, StringType
        
        # This is a simplified implementation that needs to be expanded
        # to handle all Iceberg field types
        fields = []
        
        for field_json in schema_json.get('fields', []):
            # For this simplified version, we'll just create string type fields
            # A real implementation would parse the type string and create appropriate type objects
            field_id = field_json.get('id')
            name = field_json.get('name')
            required = field_json.get('required', False)
            doc = field_json.get('doc')
            
            # Create a field with a string type for demonstration
            # In a real implementation, parse the type string to create the correct type
            field = NestedField(field_id, name, StringType(), required, doc)
            fields.append(field)
        
        # Create a schema from the fields
        return Schema(*fields)