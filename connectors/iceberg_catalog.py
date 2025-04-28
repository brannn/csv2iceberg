"""
Iceberg catalog manager for S3 Tables integration
"""
import os
import json
import logging
import uuid
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class IcebergCatalog:
    """Manager for Iceberg catalog operations"""
    
    def __init__(self, table_location: str, s3_client: Any):
        """Initialize the Iceberg catalog manager.
        
        Args:
            table_location: Base location for table data (S3 URI)
            s3_client: S3 client for file operations
        """
        self.table_location = table_location.rstrip('/')
        self.s3_client = s3_client
        logger.debug(f"Initialized Iceberg catalog manager for table location: {table_location}")
    
    def _generate_manifest_path(self) -> str:
        """Generate a path for a new manifest file.
        
        Returns:
            S3 URI for the manifest file
        """
        manifest_id = str(uuid.uuid4())
        return f"{self.table_location}/metadata/manifest-{manifest_id}.avro"
    
    def _generate_data_file_path(self) -> str:
        """Generate a path for a new data file.
        
        Returns:
            S3 URI for the data file
        """
        file_id = str(uuid.uuid4())
        return f"{self.table_location}/data/{file_id}.parquet"
    
    def _write_manifest_file(self, manifest: Dict[str, Any], data_files: List[Dict[str, Any]]) -> Tuple[str, int]:
        """Write a manifest file to S3.
        
        Args:
            manifest: Manifest file entry
            data_files: List of data file entries
            
        Returns:
            Tuple of (manifest path, manifest length)
        """
        # Create manifest schema
        schema = {
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "data_file", "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {"name": "partition", "type": {"type": "map", "values": "string"}},
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {"name": "column_sizes", "type": {"type": "map", "values": "long"}},
                        {"name": "value_counts", "type": {"type": "map", "values": "long"}},
                        {"name": "null_value_counts", "type": {"type": "map", "values": "long"}},
                        {"name": "nan_value_counts", "type": {"type": "map", "values": "long"}},
                        {"name": "lower_bounds", "type": {"type": "map", "values": "bytes"}},
                        {"name": "upper_bounds", "type": {"type": "map", "values": "bytes"}},
                        {"name": "key_metadata", "type": ["null", "bytes"]},
                        {"name": "split_offsets", "type": {"type": "array", "items": "long"}},
                        {"name": "sort_order_id", "type": "int"}
                    ]
                }}
            ]
        }
        
        # Create manifest entries
        entries = []
        for data_file in data_files:
            entry = {
                "status": 1,  # ADDED
                "snapshot_id": manifest["added_snapshot_id"],
                "data_file": data_file
            }
            entries.append(entry)
        
        # Write manifest file
        manifest_path = manifest["manifest_path"]
        bucket = manifest_path.split('/')[2]
        key = '/'.join(manifest_path.split('/')[3:])
        
        with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as temp_file:
            fastavro.writer(temp_file, schema, entries)
            temp_file.flush()
            
            # Upload to S3
            with open(temp_file.name, 'rb') as f:
                self.s3_client.upload_fileobj(f, bucket, key)
            
            # Get file size
            manifest_length = os.path.getsize(temp_file.name)
            
            # Clean up
            os.unlink(temp_file.name)
        
        return manifest_path, manifest_length
    
    def create_manifest(self, data_files: List[Dict[str, Any]], snapshot_id: int) -> Dict[str, Any]:
        """Create a new manifest file.
        
        Args:
            data_files: List of data file entries
            snapshot_id: ID of the snapshot this manifest belongs to
            
        Returns:
            Manifest file entry
        """
        manifest_path = self._generate_manifest_path()
        manifest_id = os.path.basename(manifest_path).replace('.avro', '')
        
        # Create manifest entry
        manifest = {
            "manifest_path": manifest_path,
            "manifest_length": 0,  # Will be updated after writing
            "partition_spec_id": 0,
            "content": 0,  # DATA
            "sequence_number": 0,
            "min_sequence_number": 0,
            "added_snapshot_id": snapshot_id,
            "added_files_count": len(data_files),
            "existing_files_count": 0,
            "deleted_files_count": 0,
            "added_rows_count": sum(f.get("record_count", 0) for f in data_files),
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
            "partitions": [],
            "key_metadata": None
        }
        
        # Write manifest file
        manifest_path, manifest_length = self._write_manifest_file(manifest, data_files)
        manifest["manifest_length"] = manifest_length
        
        logger.debug(f"Created manifest entry: {manifest}")
        return manifest
    
    def create_snapshot(self, manifest: Dict[str, Any], 
                       parent_snapshot_id: Optional[int] = None,
                       operation: str = "append") -> Dict[str, Any]:
        """Create a new snapshot.
        
        Args:
            manifest: Manifest file entry
            parent_snapshot_id: Optional parent snapshot ID
            operation: Operation type (append, delete, etc.)
            
        Returns:
            Snapshot entry
        """
        snapshot_id = int(datetime.now().timestamp() * 1000)
        
        # Create snapshot entry
        snapshot = {
            "snapshot_id": snapshot_id,
            "timestamp_ms": snapshot_id,
            "manifest_list": manifest["manifest_path"],
            "summary": {
                "operation": operation,
                "added-data-files": manifest["added_files_count"],
                "added-records": manifest["added_rows_count"],
                "added-files-size": 0,  # Will be updated after writing
                "changed-partition-count": 0,
                "total-records": manifest["added_rows_count"],
                "total-files-size": 0,  # Will be updated after writing
                "total-data-files": manifest["added_files_count"],
                "total-delete-files": 0,
                "total-position-deletes": 0,
                "total-equality-deletes": 0
            }
        }
        
        if parent_snapshot_id is not None:
            snapshot["parent_snapshot_id"] = parent_snapshot_id
        
        logger.debug(f"Created snapshot entry: {snapshot}")
        return snapshot
    
    def create_data_file(self, df: pa.Table, partition: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a new data file entry.
        
        Args:
            df: PyArrow table
            partition: Optional partition information
            
        Returns:
            Data file entry
        """
        data_path = self._generate_data_file_path()
        
        # Calculate column statistics
        column_sizes = {}
        value_counts = {}
        null_value_counts = {}
        nan_value_counts = {}
        lower_bounds = {}
        upper_bounds = {}
        
        for field in df.schema:
            col = df.column(field.name)
            column_sizes[field.name] = col.nbytes
            value_counts[field.name] = len(col)
            null_value_counts[field.name] = col.null_count
            nan_value_counts[field.name] = 0  # TODO: Implement NaN counting
            
            # Calculate bounds for numeric types
            if pa.types.is_numeric(field.type):
                lower_bounds[field.name] = str(col.min()).encode()
                upper_bounds[field.name] = str(col.max()).encode()
        
        # Create data file entry
        data_file = {
            "content": 0,  # DATA
            "file_path": data_path,
            "file_format": "PARQUET",
            "partition": partition or {},
            "record_count": len(df),
            "file_size_in_bytes": 0,  # Will be updated after writing
            "column_sizes": column_sizes,
            "value_counts": value_counts,
            "null_value_counts": null_value_counts,
            "nan_value_counts": nan_value_counts,
            "lower_bounds": lower_bounds,
            "upper_bounds": upper_bounds,
            "key_metadata": None,
            "split_offsets": [],
            "sort_order_id": 0
        }
        
        logger.debug(f"Created data file entry: {data_file}")
        return data_file
    
    def create_commit(self, snapshot: Dict[str, Any], 
                     manifest: Dict[str, Any],
                     data_files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a commit operation.
        
        Args:
            snapshot: Snapshot entry
            manifest: Manifest file entry
            data_files: List of data file entries
            
        Returns:
            Commit operation
        """
        # Calculate total file size
        total_size = sum(f.get("file_size_in_bytes", 0) for f in data_files)
        
        # Update snapshot summary
        snapshot["summary"]["added-files-size"] = total_size
        snapshot["summary"]["total-files-size"] = total_size
        
        # Create commit operation
        commit = {
            "type": snapshot["summary"]["operation"],
            "snapshot_id": snapshot["snapshot_id"],
            "manifest_list": snapshot["manifest_list"],
            "manifests": [manifest],
            "data_files": data_files
        }
        
        logger.debug(f"Created commit operation: {commit}")
        return commit
    
    def get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot for the table.
        
        Returns:
            Latest snapshot entry or None if no snapshots exist
        """
        try:
            # Read the metadata file
            bucket = self.table_location.split('/')[2]
            key = f"{'/'.join(self.table_location.split('/')[3:])}/metadata/metadata.json"
            
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            metadata = json.loads(response['Body'].read().decode())
            
            # Get the latest snapshot
            if "current-snapshot-id" in metadata:
                snapshot_id = metadata["current-snapshot-id"]
                for snapshot in metadata["snapshots"]:
                    if snapshot["snapshot-id"] == snapshot_id:
                        return snapshot
            
            return None
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
    
    def update_metadata(self, commit: Dict[str, Any]) -> None:
        """Update the table metadata with a new commit.
        
        Args:
            commit: Commit operation
        """
        try:
            # Read the current metadata
            bucket = self.table_location.split('/')[2]
            key = f"{'/'.join(self.table_location.split('/')[3:])}/metadata/metadata.json"
            
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                metadata = json.loads(response['Body'].read().decode())
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    metadata = {
                        "format-version": 2,
                        "table-uuid": str(uuid.uuid4()),
                        "location": self.table_location,
                        "last-updated-ms": int(datetime.now().timestamp() * 1000),
                        "last-column-id": 0,
                        "schemas": [],
                        "current-schema-id": 0,
                        "partition-specs": [],
                        "default-partition-spec-id": 0,
                        "last-partition-id": 0,
                        "properties": {},
                        "current-snapshot-id": None,
                        "snapshots": [],
                        "snapshot-log": [],
                        "metadata-log": []
                    }
                else:
                    raise
            
            # Update metadata
            metadata["last-updated-ms"] = int(datetime.now().timestamp() * 1000)
            metadata["current-snapshot-id"] = commit["snapshot_id"]
            
            # Add snapshot
            snapshot = {
                "snapshot-id": commit["snapshot_id"],
                "timestamp-ms": int(datetime.now().timestamp() * 1000),
                "summary": commit["snapshot"]["summary"],
                "manifest-list": commit["manifest_list"],
                "schema-id": 0
            }
            
            if "parent_snapshot_id" in commit["snapshot"]:
                snapshot["parent-snapshot-id"] = commit["snapshot"]["parent_snapshot_id"]
            
            metadata["snapshots"].append(snapshot)
            
            # Add snapshot log entry
            snapshot_log = {
                "timestamp-ms": snapshot["timestamp-ms"],
                "snapshot-id": snapshot["snapshot-id"]
            }
            metadata["snapshot-log"].append(snapshot_log)
            
            # Write updated metadata
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(metadata, indent=2).encode()
            )
            
            logger.debug(f"Updated table metadata with commit: {commit}")
            
        except ClientError as e:
            logger.error(f"Failed to update table metadata: {str(e)}")
            raise 