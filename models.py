"""
Database models for the CSV to Iceberg application.
"""
import datetime
import enum
import json
from typing import Dict, List, Optional, Any

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, JSON
from sqlalchemy.orm import relationship

# Import the SQLAlchemy instance from main.py
from main import db

class CompatibilityLevel(enum.Enum):
    """Compatibility levels for schema changes."""
    FULL = "FULL"  # Fully compatible, no data loss
    BACKWARD = "BACKWARD"  # Can read old data with new schema
    FORWARD = "FORWARD"  # Can read new data with old schema
    NONE = "NONE"  # Incompatible

class ChangeType(enum.Enum):
    """Types of schema changes."""
    ADD_COLUMN = "ADD_COLUMN"
    REMOVE_COLUMN = "REMOVE_COLUMN"
    CHANGE_TYPE = "CHANGE_TYPE"
    RENAME_COLUMN = "RENAME_COLUMN"
    CHANGE_NULLABILITY = "CHANGE_NULLABILITY"
    OTHER = "OTHER"

class SchemaVersion(db.Model):
    """Model for tracking schema versions."""
    __tablename__ = "schema_versions"

    id = Column(Integer, primary_key=True)
    table_name = Column(Text, nullable=False)
    catalog_name = Column(Text, nullable=False)
    schema_name = Column(Text, nullable=False)
    version_number = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    created_by = Column(Text)
    schema_definition = Column(JSON, nullable=False)
    change_description = Column(Text)
    compatibility_level = Column(
        Text, 
        nullable=False, 
        default=CompatibilityLevel.FULL.value
    )

    # Relationships
    changes = relationship("SchemaChange", back_populates="schema_version", cascade="all, delete-orphan")

    @property
    def full_table_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
    
    @staticmethod
    def get_latest_version(catalog: str, schema: str, table: str) -> Optional['SchemaVersion']:
        """Get the latest schema version for a table."""
        return SchemaVersion.query.filter_by(
            catalog_name=catalog,
            schema_name=schema,
            table_name=table
        ).order_by(SchemaVersion.version_number.desc()).first()
    
    @staticmethod
    def get_version_history(catalog: str, schema: str, table: str) -> List['SchemaVersion']:
        """Get all schema versions for a table, ordered by version."""
        return SchemaVersion.query.filter_by(
            catalog_name=catalog,
            schema_name=schema,
            table_name=table
        ).order_by(SchemaVersion.version_number).all()
    
    @staticmethod
    def create_from_iceberg_schema(
        catalog: str, 
        schema: str, 
        table: str, 
        iceberg_schema: Any,  # PyIceberg Schema object
        created_by: Optional[str] = None,
        change_description: Optional[str] = None,
        compatibility_level: CompatibilityLevel = CompatibilityLevel.FULL
    ) -> 'SchemaVersion':
        """
        Create a new schema version from an Iceberg schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            iceberg_schema: PyIceberg Schema object
            created_by: User who created this version
            change_description: Description of the changes
            compatibility_level: Compatibility level of this schema change
            
        Returns:
            New SchemaVersion object
        """
        # Find the latest version number
        latest_version = SchemaVersion.get_latest_version(catalog, schema, table)
        version_number = 1 if latest_version is None else latest_version.version_number + 1
        
        # Convert Iceberg schema to JSON
        schema_json = SchemaVersion._iceberg_schema_to_json(iceberg_schema)
        
        # Create the new version
        new_version = SchemaVersion(
            catalog_name=catalog,
            schema_name=schema,
            table_name=table,
            version_number=version_number,
            created_by=created_by,
            schema_definition=schema_json,
            change_description=change_description,
            compatibility_level=compatibility_level.value
        )
        
        db.session.add(new_version)
        db.session.commit()
        
        return new_version
    
    @staticmethod
    def _iceberg_schema_to_json(iceberg_schema) -> Dict[str, Any]:
        """
        Convert a PyIceberg Schema object to a JSON-compatible dictionary.
        
        Args:
            iceberg_schema: PyIceberg Schema object
            
        Returns:
            Dictionary representation of the schema
        """
        fields = []
        for field in iceberg_schema.fields:
            fields.append({
                "id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc
            })
        
        return {
            "type": "struct",
            "fields": fields,
            "schema_id": getattr(iceberg_schema, 'schema_id', None)
        }

class SchemaChange(db.Model):
    """Model for tracking individual schema changes."""
    __tablename__ = "schema_changes"

    id = Column(Integer, primary_key=True)
    schema_version_id = Column(Integer, ForeignKey("schema_versions.id"), nullable=False)
    field_name = Column(Text, nullable=False)
    change_type = Column(Text, nullable=False)
    prev_type = Column(Text)
    new_type = Column(Text)
    compatibility = Column(Text, nullable=False)
    change_description = Column(Text)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    # Relationships
    schema_version = relationship("SchemaVersion", back_populates="changes")
    
    @staticmethod
    def create_change(
        schema_version_id: int,
        field_name: str,
        change_type: ChangeType,
        compatibility: CompatibilityLevel,
        prev_type: Optional[str] = None,
        new_type: Optional[str] = None,
        change_description: Optional[str] = None
    ) -> 'SchemaChange':
        """
        Create a schema change record.
        
        Args:
            schema_version_id: ID of the schema version
            field_name: Name of the field that changed
            change_type: Type of change
            compatibility: Compatibility level of this change
            prev_type: Previous type (for type changes)
            new_type: New type (for type changes)
            change_description: Description of the change
            
        Returns:
            New SchemaChange object
        """
        change = SchemaChange(
            schema_version_id=schema_version_id,
            field_name=field_name,
            change_type=change_type.value,
            compatibility=compatibility.value,
            prev_type=prev_type,
            new_type=new_type,
            change_description=change_description
        )
        
        db.session.add(change)
        db.session.commit()
        
        return change