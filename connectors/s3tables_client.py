"""
S3 Tables client for high-level table operations
"""
import logging
from typing import Dict, Any, List, Optional, Union
import pyarrow as pa
from connectors.iceberg_rest_client import IcebergRestClient
from connectors.s3_parquet_writer import S3ParquetWriter
from connectors.iceberg_catalog import IcebergCatalog
from connectors.iceberg_metadata import IcebergMetadata

logger = logging.getLogger(__name__)

class S3TablesClient:
    """Client for S3 Tables operations"""
    
    def __init__(self, endpoint: str, region: str, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None):
        """Initialize the S3 Tables client.
        
        Args:
            endpoint: Iceberg REST Catalog endpoint
            region: AWS region
            aws_access_key_id: Optional AWS access key ID
            aws_secret_access_key: Optional AWS secret access key
        """
        self.rest_client = IcebergRestClient(
            endpoint=endpoint,
            region=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        self.s3_writer = S3ParquetWriter(
            region=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        logger.debug(f"Initialized S3 Tables client for endpoint: {endpoint}")
    
    def create_table(self, namespace: str, table_name: str, schema: pa.Schema,
                    partition_fields: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """Create a new table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            schema: PyArrow schema
            partition_fields: Optional partition fields
            
        Returns:
            Table metadata
            
        Raises:
            Exception: If table creation fails
        """
        try:
            # Convert schema to Iceberg format
            metadata = IcebergMetadata()
            iceberg_schema = metadata.convert_arrow_schema(schema)
            
            # Create partition spec if needed
            partition_spec = None
            if partition_fields:
                partition_spec = metadata.create_partition_spec(iceberg_schema, partition_fields)
            
            # Create table properties
            properties = metadata.create_table_properties()
            
            # Create the table
            table = self.rest_client.create_table(
                namespace=namespace,
                table_name=table_name,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(f"Created table {namespace}.{table_name}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to create table {namespace}.{table_name}: {str(e)}")
            raise
    
    def insert_data(self, namespace: str, table_name: str, data: pa.Table,
                   partition: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Insert data into a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            data: PyArrow table
            partition: Optional partition information
            
        Returns:
            Insert operation result
            
        Raises:
            Exception: If insert operation fails
        """
        try:
            # Get table metadata
            table = self.rest_client.get_table(namespace, table_name)
            
            # Create catalog manager
            catalog = IcebergCatalog(
                table_location=table["location"],
                s3_client=self.s3_writer.s3_client
            )
            
            # Create data file
            data_file = catalog.create_data_file(data, partition)
            
            # Write data to S3
            data_path = data_file["file_path"]
            bucket = data_path.split('/')[2]
            key = '/'.join(data_path.split('/')[3:])
            
            self.s3_writer.write_dataframe(
                df=data,
                bucket=bucket,
                key=key
            )
            
            # Get latest snapshot
            latest_snapshot = catalog.get_latest_snapshot()
            snapshot_id = latest_snapshot["snapshot-id"] if latest_snapshot else None
            
            # Create manifest
            manifest = catalog.create_manifest([data_file], snapshot_id)
            
            # Create snapshot
            snapshot = catalog.create_snapshot(
                manifest=manifest,
                parent_snapshot_id=snapshot_id,
                operation="append"
            )
            
            # Create commit
            commit = catalog.create_commit(snapshot, manifest, [data_file])
            
            # Update metadata
            catalog.update_metadata(commit)
            
            logger.info(f"Inserted {len(data)} rows into {namespace}.{table_name}")
            return commit
            
        except Exception as e:
            logger.error(f"Failed to insert data into {namespace}.{table_name}: {str(e)}")
            raise
    
    def batch_insert(self, namespace: str, table_name: str, data: pa.Table,
                    batch_size: int = 100000,
                    partition: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Insert data in batches.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            data: PyArrow table
            batch_size: Number of rows per batch
            partition: Optional partition information
            
        Returns:
            List of insert operation results
            
        Raises:
            Exception: If batch insert operation fails
        """
        try:
            results = []
            num_rows = len(data)
            num_batches = (num_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                start = i * batch_size
                end = min((i + 1) * batch_size, num_rows)
                
                # Get batch
                batch = data.slice(start, end - start)
                
                # Insert batch
                result = self.insert_data(
                    namespace=namespace,
                    table_name=table_name,
                    data=batch,
                    partition=partition
                )
                
                results.append(result)
                logger.info(f"Inserted batch {i+1}/{num_batches} into {namespace}.{table_name}")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to batch insert data into {namespace}.{table_name}: {str(e)}")
            raise
    
    def delete_table(self, namespace: str, table_name: str) -> None:
        """Delete a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Raises:
            Exception: If table deletion fails
        """
        try:
            self.rest_client.delete_table(namespace, table_name)
            logger.info(f"Deleted table {namespace}.{table_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete table {namespace}.{table_name}: {str(e)}")
            raise 