"""
Trino client module for CSV to Iceberg conversion
"""
import logging
import socket
import time
import polars as pl
import pyarrow as pa
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import quote_plus

# Import PyIceberg schema and types
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DateType,
    TimestampType,
    StringType,
    DecimalType,
    StructType,
    NestedField as Field
)

# Import Trino client libraries
import trino

logger = logging.getLogger(__name__)

class TrinoClient:
    """Client for interacting with Trino server"""
    
    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = 'admin',
        password: Optional[str] = None,
        catalog: str = 'hive',
        schema: str = 'default',
        http_scheme: str = 'http',
        role: str = 'sysadmin'
    ):
        """
        Initialize Trino client.
        
        Args:
            host: Trino host
            port: Trino port
            user: Trino user
            password: Trino password (if authentication is enabled)
            catalog: Default catalog
            schema: Default schema
            http_scheme: HTTP scheme (http or https)
            role: Trino role to use (e.g., 'sysadmin')
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        self.role = role
        
        # Schema cache dict: {(catalog, schema, table): [(column_name, column_type), ...]}
        self._schema_cache = {}
        
        # Table existence cache dict: {(catalog, schema, table): bool}
        self._table_existence_cache = {}
        
        self.connection = self._create_connection()
        
    def _create_connection(self):
        """Create a connection to Trino"""
        try:
            auth_msg = "with authentication" if self.password else "without authentication"
            logger.info(f"Connecting to Trino at {self.host}:{self.port} as user '{self.user}' {auth_msg}")
            
            # Test if the host/port is available
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            
            if result != 0:
                error_msg = f"Trino server at {self.host}:{self.port} is not available"
                logger.error(error_msg)
                raise ConnectionError(error_msg)
            
            # Create the real Trino connection
            auth = None
            
            # Set up HTTP headers with Trino role
            http_headers = {
                'x-trino-role': f'system=ROLE{{{self.role}}}'
            }
            logger.info(f"Using Trino role: {self.role}")
            
            conn_args = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'catalog': self.catalog,
                'schema': self.schema,
                'http_scheme': self.http_scheme,
                'http_headers': http_headers,
                'verify': False,  # For testing purposes, don't verify SSL
                'request_timeout': 120  # Longer timeout for large operations
                # No custom session properties - keep it simple
            }
            
            # Handle auth correctly based on http_scheme
            if self.password:
                if self.http_scheme.lower() == 'https':
                    # Only use authentication with HTTPS
                    auth = trino.auth.BasicAuthentication(self.user, self.password)
                    conn_args['auth'] = auth
                else:
                    error_msg = "Password authentication requires HTTPS. Cannot use HTTP with authentication."
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            
            # Create connection with appropriate settings            
            conn = trino.dbapi.connect(**conn_args)
            
            logger.info(f"Successfully connected to Trino at {self.host}:{self.port}")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}", exc_info=True)
            raise ConnectionError(f"Failed to connect to Trino: {str(e)}")
    
    def execute_query(self, query: str) -> List[Tuple]:
        """
        Execute a query on Trino.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result rows
        """
        try:
            logger.info(f"Executing query on Trino: {query}")
            
            if self.connection is None:
                error_msg = "No active Trino connection"
                logger.error(error_msg)
                raise ConnectionError(error_msg)
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # If the query returns results, fetch them
            if cursor.description:
                results = cursor.fetchall()
                cursor.close()
                logger.info(f"Query returned {len(results)} rows")
                return results
            
            cursor.close()
            return []
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to execute query: {str(e)}")
    
    def table_exists(self, catalog: str, schema: str, table: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            True if the table exists
        """
        try:
            cache_key = (catalog, schema, table)
            
            # Check cache first
            if cache_key in self._table_existence_cache:
                cached_result = self._table_existence_cache[cache_key]
                logger.debug(f"Using cached result for table_exists({catalog}.{schema}.{table}): {cached_result}")
                return cached_result
            
            logger.info(f"Checking if table exists: {catalog}.{schema}.{table}")
            
            if self.connection is None:
                error_msg = "No active Trino connection"
                logger.error(error_msg)
                raise ConnectionError(error_msg)
                
            # Query the information schema to check if the table exists
            query = f"""
            SELECT table_name 
            FROM {catalog}.information_schema.tables 
            WHERE table_catalog = '{catalog}' 
              AND table_schema = '{schema}' 
              AND table_name = '{table}'
            """
            
            results = self.execute_query(query)
            exists = len(results) > 0
            
            # Cache the result
            self._table_existence_cache[cache_key] = exists
            
            logger.info(f"Table {catalog}.{schema}.{table} {'exists' if exists else 'does not exist'}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            raise RuntimeError(f"Failed to check if table exists: {str(e)}")
    
    def _clear_cache_for_table(self, catalog: str, schema: str, table: str) -> None:
        """
        Clear caches for a specific table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
        """
        cache_key = (catalog, schema, table)
        
        # Clear schema cache
        if cache_key in self._schema_cache:
            logger.debug(f"Clearing schema cache for {catalog}.{schema}.{table}")
            del self._schema_cache[cache_key]
        
        # Clear table existence cache
        if cache_key in self._table_existence_cache:
            logger.debug(f"Clearing table existence cache for {catalog}.{schema}.{table}")
            del self._table_existence_cache[cache_key]
    
    def create_iceberg_table(
        self, 
        catalog: str, 
        schema: str, 
        table: str, 
        iceberg_schema: Schema
    ) -> None:
        """
        Create a table using the provided schema. Originally designed for Iceberg tables,
        but may create tables with other formats (like Parquet) based on Trino server support.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            iceberg_schema: PyIceberg Schema object
        """
        try:
            # Clear any cached data for this table first
            self._clear_cache_for_table(catalog, schema, table)
            
            # Convert PyIceberg schema to Trino DDL
            columns_ddl = []
            for field in iceberg_schema.fields:
                # Properly quote column names to handle special characters and spaces
                column_name = f'"{field.name}"'
                column_type = iceberg_type_to_trino_type(field.field_type)
                columns_ddl.append(f"{column_name} {column_type}")
            
            columns_clause = ", ".join(columns_ddl)
            
            # Create table DDL
            # Use PARQUET format since 'ICEBERG' format is not supported in this Trino instance
            create_table_sql = f"""
            CREATE TABLE {catalog}.{schema}.{table} (
                {columns_clause}
            )
            WITH (
                format = 'PARQUET'
            )
            """
            
            logger.info(f"Creating table {catalog}.{schema}.{table} with PARQUET format")
            logger.debug(f"Create table SQL: {create_table_sql}")
            
            self.execute_query(create_table_sql)
            
            # Update the cache to indicate the table now exists
            self._table_existence_cache[(catalog, schema, table)] = True
            
            logger.info(f"Successfully created table: {catalog}.{schema}.{table}")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to create table: {str(e)}")
    
    def drop_table(self, catalog: str, schema: str, table: str) -> None:
        """
        Drop a table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
        """
        try:
            # Clear any cached data for this table first
            self._clear_cache_for_table(catalog, schema, table)
            
            drop_table_sql = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
            logger.info(f"Dropping table: {catalog}.{schema}.{table}")
            self.execute_query(drop_table_sql)
            
            # Update the cache to indicate the table no longer exists
            self._table_existence_cache[(catalog, schema, table)] = False
            
            logger.info(f"Successfully dropped table: {catalog}.{schema}.{table}")
        except Exception as e:
            logger.error(f"Error dropping table: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to drop table: {str(e)}")
    
    def get_table_schema(self, catalog: str, schema: str, table: str) -> List[Tuple[str, str]]:
        """
        Get the schema of a table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            List of (column_name, column_type) tuples
        """
        try:
            cache_key = (catalog, schema, table)
            
            # Check cache first
            if cache_key in self._schema_cache:
                cached_schema = self._schema_cache[cache_key]
                logger.debug(f"Using cached schema for {catalog}.{schema}.{table}: {len(cached_schema)} columns")
                return cached_schema
                
            logger.info(f"Fetching schema for {catalog}.{schema}.{table}")
            
            query = f"""
            SELECT column_name, data_type 
            FROM {catalog}.information_schema.columns 
            WHERE table_catalog = '{catalog}' 
              AND table_schema = '{schema}' 
              AND table_name = '{table}'
            ORDER BY ordinal_position
            """
            
            result = self.execute_query(query)
            
            # Cache the result
            self._schema_cache[cache_key] = result
            logger.info(f"Cached schema for {catalog}.{schema}.{table}: {len(result)} columns")
            
            return result
        except Exception as e:
            logger.error(f"Error getting table schema: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to get table schema: {str(e)}")
    
    def validate_table_schema(
        self, 
        catalog: str, 
        schema: str, 
        table: str, 
        iceberg_schema: Schema
    ) -> bool:
        """
        Validate that an existing table's schema is compatible with the inferred schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            iceberg_schema: PyIceberg Schema object
            
        Returns:
            True if the schemas are compatible
        """
        try:
            existing_schema = self.get_table_schema(catalog, schema, table)
            
            # Create a map of column name to type for the existing schema
            existing_columns = {col_name.strip('"').lower(): col_type for col_name, col_type in existing_schema}
            
            # Count column matches and type mismatches to determine compatibility
            matched_columns = 0
            type_mismatches = 0
            total_columns = len(iceberg_schema.fields)
            
            # For each inferred column, check if it exists in the table
            for field in iceberg_schema.fields:
                column_name = field.name.lower()
                inferred_type = iceberg_type_to_trino_type(field.field_type)
                
                # Check if column exists (with case-insensitive matching)
                if column_name in existing_columns:
                    matched_columns += 1
                    
                    # Check type compatibility
                    existing_type = existing_columns[column_name]
                    if not are_types_compatible(existing_type, inferred_type):
                        logger.warning(
                            f"Column {field.name} type mismatch: existing={existing_type}, inferred={inferred_type}"
                        )
                        type_mismatches += 1
                else:
                    logger.warning(f"Column {field.name} from inferred schema doesn't exist in table")
            
            # Schema is compatible if we've matched most columns and 
            # don't have too many type mismatches
            column_match_percent = matched_columns / total_columns if total_columns > 0 else 0
            
            # Define thresholds for schema compatibility
            MIN_COLUMN_MATCH_PERCENT = 0.7  # At least 70% of columns must match
            MAX_TYPE_MISMATCH_PERCENT = 0.3  # No more than 30% can have type mismatches
            
            is_compatible = (
                column_match_percent >= MIN_COLUMN_MATCH_PERCENT and
                (type_mismatches / total_columns if total_columns > 0 else 0) <= MAX_TYPE_MISMATCH_PERCENT
            )
            
            logger.info(
                f"Schema compatibility: {is_compatible} "
                f"(matched {matched_columns}/{total_columns} columns, "
                f"{type_mismatches} type mismatches)"
            )
            
            return is_compatible
        except Exception as e:
            logger.error(f"Error validating table schema: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to validate table schema: {str(e)}")

def iceberg_type_to_trino_type(iceberg_type: Any) -> str:
    """
    Convert a PyIceberg type to a Trino SQL type string.
    
    Args:
        iceberg_type: PyIceberg type
        
    Returns:
        Trino SQL type string
    """
    # Import PyIceberg types directly
    from pyiceberg.types import (
        BooleanType, IntegerType, LongType, FloatType, DoubleType, 
        DateType, TimestampType, StringType, DecimalType, StructType
    )
    
    # Return SQL types based on the instance type
    if isinstance(iceberg_type, BooleanType):
        return 'BOOLEAN'
    elif isinstance(iceberg_type, IntegerType):
        return 'INTEGER'
    elif isinstance(iceberg_type, LongType):
        return 'BIGINT'
    elif isinstance(iceberg_type, FloatType):
        return 'REAL'
    elif isinstance(iceberg_type, DoubleType):
        return 'DOUBLE'
    elif isinstance(iceberg_type, DateType):
        return 'DATE'
    elif isinstance(iceberg_type, TimestampType):
        # In Trino, we need to specify the type with precision to avoid syntax errors
        # Using TIMESTAMP(3) which is for millisecond precision
        return 'TIMESTAMP(3)'
    elif isinstance(iceberg_type, StringType):
        return 'VARCHAR'
    elif isinstance(iceberg_type, DecimalType):
        precision = getattr(iceberg_type, 'precision', 38)
        scale = getattr(iceberg_type, 'scale', 18)
        return f'DECIMAL({precision}, {scale})'
    elif isinstance(iceberg_type, StructType):
        return 'ROW()'
    else:
        # Default to VARCHAR for unknown types
        return 'VARCHAR'

def are_types_compatible(existing_type: str, inferred_type: str) -> bool:
    """
    Check if two Trino SQL types are compatible.
    
    Args:
        existing_type: Existing column type
        inferred_type: Inferred column type
        
    Returns:
        True if the types are compatible
    """
    # Normalize types by removing parameters and converting to uppercase
    def normalize_type(type_str):
        # Extract the base type (remove parameters)
        base_type = type_str.split('(')[0].upper()
        return base_type.strip()
    
    existing_type_norm = normalize_type(existing_type)
    inferred_type_norm = normalize_type(inferred_type)
    
    # If normalized types are the same, they're compatible
    if existing_type_norm == inferred_type_norm:
        return True
    
    # Special case for timestamp/date types - timestamp types with different precisions are compatible
    if ('TIMESTAMP' in existing_type_norm and 'TIMESTAMP' in inferred_type_norm) or \
       ('DATE' in existing_type_norm and 'DATE' in inferred_type_norm):
        return True
    
    # Define type compatibility rules - more flexible for our specific use case
    compatible_types = {
        'INTEGER': ['BIGINT', 'DOUBLE', 'DECIMAL', 'VARCHAR'],
        'BIGINT': ['DOUBLE', 'DECIMAL', 'VARCHAR', 'INTEGER'],
        'REAL': ['DOUBLE', 'VARCHAR'],
        'DOUBLE': ['VARCHAR'],
        'VARCHAR': ['CHAR', 'INTEGER', 'BIGINT', 'DOUBLE', 'BOOLEAN', 'DATE', 'TIMESTAMP'],
        'CHAR': ['VARCHAR', 'INTEGER', 'BIGINT'],
        'DATE': ['VARCHAR', 'TIMESTAMP'],
        'TIMESTAMP': ['VARCHAR', 'DATE'],
        'BOOLEAN': ['VARCHAR'],
    }
    
    # Check if inferred type is in the list of compatible types for the existing type
    if existing_type_norm in compatible_types:
        return inferred_type_norm in compatible_types[existing_type_norm]
    
    # For our specific use case, we'll be more permissive with append mode
    # We allow VARCHAR to be compatible with any type to handle edge cases
    if existing_type_norm == 'VARCHAR' or inferred_type_norm == 'VARCHAR':
        return True
    
    # Default to allowing compatibility for append mode
    # This is a more permissive approach that prioritizes functionality over strict type checking
    logger.warning(
        f"Using default compatibility for types {existing_type} and {inferred_type}"
    )
    return True
