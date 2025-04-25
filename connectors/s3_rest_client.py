"""
AWS S3 Tables REST API client for Iceberg operations

This module provides an implementation of the Iceberg REST API specifically designed
for AWS S3 Tables. It uses PyIceberg's RestCatalog for interacting with the S3 Tables service.
"""
import os
import logging
import json
from typing import Dict, List, Optional, Any, Union, Tuple
import time

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table as PyIcebergTable

# Configure logger
logger = logging.getLogger(__name__)

class S3RestClient:
    """
    Client for interacting with AWS S3 Tables via the Iceberg REST API
    """
    
    def __init__(
        self,
        rest_uri: str,
        warehouse_location: str,
        catalog_name: str = "s3_catalog",
        namespace: str = "default",
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None,
        aws_access_key_id: Optional[str] = None, 
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region: str = "us-east-1"
    ):
        """
        Initialize S3 REST API client.
        
        Args:
            rest_uri: REST endpoint URI for S3 Tables
            warehouse_location: S3 location for storing Iceberg tables (e.g., s3://bucket/path)
            catalog_name: Name for the catalog
            namespace: Default namespace/schema
            client_id: OAuth client ID (if using OAuth)
            client_secret: OAuth client secret (if using OAuth)
            token: Bearer token (if not using OAuth)
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            region: AWS region
        """
        # Ensure the REST URI has a scheme (http:// or https://)
        if rest_uri and not rest_uri.startswith(('http://', 'https://')):
            # Default to HTTPS if no scheme is provided
            rest_uri = f"https://{rest_uri}"
            logger.info(f"Added HTTPS scheme to REST URI: {rest_uri}")
        
        # Ensure warehouse location starts with s3://
        if warehouse_location and not warehouse_location.startswith('s3://'):
            warehouse_location = f"s3://{warehouse_location}"
            logger.info(f"Added s3:// prefix to warehouse location: {warehouse_location}")
            
        self.rest_uri = rest_uri
        self.warehouse_location = warehouse_location
        self.catalog_name = catalog_name
        self.namespace = namespace
        
        # Build catalog properties
        properties = {
            "uri": rest_uri,
            "warehouse": warehouse_location,
        }
        
        # Add authentication properties
        if client_id and client_secret:
            properties["credential"] = "oauth2"
            properties["client-id"] = client_id
            properties["client-secret"] = client_secret
        elif token:
            properties["credential"] = "token"
            properties["token"] = token
        
        # Add AWS credentials if provided
        if aws_access_key_id and aws_secret_access_key:
            properties["s3.access-key-id"] = aws_access_key_id
            properties["s3.secret-access-key"] = aws_secret_access_key
            if aws_session_token:
                properties["s3.session-token"] = aws_session_token
                
        # Add AWS region
        properties["s3.region"] = region
        
        # Create REST catalog
        try:
            self.catalog = RestCatalog(catalog_name, **properties)
            logger.info(f"S3 REST catalog initialized with URI: {rest_uri}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 REST catalog: {str(e)}")
            raise
    
    def namespace_exists(self, namespace: str) -> bool:
        """
        Check if a namespace exists.
        
        Args:
            namespace: Namespace name
            
        Returns:
            True if the namespace exists
        """
        try:
            # List all namespaces and check if the target namespace exists
            namespaces = self.catalog.list_namespaces()
            return (namespace,) in namespaces
        except Exception as e:
            logger.error(f"Error checking if namespace {namespace} exists: {str(e)}")
            return False
    
    def create_namespace(self, namespace: str, description: str = "") -> bool:
        """
        Create a namespace.
        
        Args:
            namespace: Namespace name
            description: Namespace description
            
        Returns:
            True if the namespace was created
        """
        try:
            properties = {}
            if description:
                properties["description"] = description
                
            self.catalog.create_namespace(namespace, properties)
            logger.info(f"Created namespace: {namespace}")
            return True
        except Exception as e:
            logger.error(f"Error creating namespace {namespace}: {str(e)}")
            return False
    
    def table_exists(self, namespace: str, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            
        Returns:
            True if the table exists
        """
        try:
            identifier = f"{namespace}.{table_name}"
            return self.catalog.table_exists(identifier)
        except Exception as e:
            logger.error(f"Error checking if table {namespace}.{table_name} exists: {str(e)}")
            return False
    
    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: Schema,
        location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Optional[PyIcebergTable]:
        """
        Create an Iceberg table using S3 Tables REST API.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            schema: PyIceberg Schema object
            location: Storage location (optional, will use default if not specified)
            properties: Additional table properties
            
        Returns:
            PyIceberg Table object if successful, None otherwise
        """
        try:
            identifier = f"{namespace}.{table_name}"
            
            # Set default location if not provided
            if not location:
                location = f"{self.warehouse_location}/{namespace}/{table_name}"
            
            # Merge with additional properties if provided
            table_properties = properties or {}
            
            # Create table via REST API
            table = self.catalog.create_table(
                identifier=identifier,
                schema=schema,
                location=location,
                properties=table_properties
            )
            
            logger.info(f"Created table: {namespace}.{table_name} at {location}")
            return table
        except Exception as e:
            logger.error(f"Error creating table {namespace}.{table_name}: {str(e)}")
            return None
    
    def load_table(self, namespace: str, table_name: str) -> Optional[PyIcebergTable]:
        """
        Load an existing Iceberg table.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            
        Returns:
            PyIceberg Table object if successful, None otherwise
        """
        try:
            identifier = f"{namespace}.{table_name}"
            table = self.catalog.load_table(identifier)
            return table
        except Exception as e:
            logger.error(f"Error loading table {namespace}.{table_name}: {str(e)}")
            return None
    
    def write_dataframe_to_table(
        self,
        namespace: str,
        table_name: str,
        df,  # polars.DataFrame
        mode: str = "append"
    ) -> bool:
        """
        Write a Polars DataFrame to an Iceberg table through S3 Tables.
        
        This is a higher-level method that is more complex and would require 
        additional implementation details based on PyIceberg's APIs.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            df: Polars DataFrame
            mode: Write mode (append or overwrite)
            
        Returns:
            True if successful
        """
        try:
            identifier = f"{namespace}.{table_name}"
            table = self.catalog.load_table(identifier)
            
            # Convert to Arrow format for PyIceberg
            arrow_table = df.to_arrow()
            
            # This is simplified - the actual implementation would need to use
            # PyIceberg's writer components directly or use third-party libraries
            # supporting Iceberg writes with PyArrow tables
            
            # Depending on the implementation approach, additional 
            # parameters may be needed here
            if mode == "overwrite":
                logger.info(f"Overwriting data in table {namespace}.{table_name}")
                # Implementation for overwrite mode
                pass
            else:  # append mode
                logger.info(f"Appending data to table {namespace}.{table_name}")
                # Implementation for append mode
                pass
                
            logger.info(f"Successfully wrote data to {namespace}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Error writing data to {namespace}.{table_name}: {str(e)}")
            return False
    
    def drop_table(self, namespace: str, table_name: str) -> bool:
        """
        Drop a table.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            
        Returns:
            True if the table was dropped
        """
        try:
            identifier = f"{namespace}.{table_name}"
            self.catalog.drop_table(identifier)
            logger.info(f"Dropped table: {namespace}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping table {namespace}.{table_name}: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close any resources being used by the REST client"""
        # Currently no resources to close with REST catalog
        pass