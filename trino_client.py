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
            
            logger.info(f"Table {catalog}.{schema}.{table} {'exists' if exists else 'does not exist'}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            raise RuntimeError(f"Failed to check if table exists: {str(e)}")
    
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
            drop_table_sql = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
            logger.info(f"Dropping table: {catalog}.{schema}.{table}")
            self.execute_query(drop_table_sql)
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
            query = f"""
            SELECT column_name, data_type 
            FROM {catalog}.information_schema.columns 
            WHERE table_catalog = '{catalog}' 
              AND table_schema = '{schema}' 
              AND table_name = '{table}'
            ORDER BY ordinal_position
            """
            return self.execute_query(query)
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
            existing_columns = {col_name.strip('"'): col_type for col_name, col_type in existing_schema}
            
            # Check that all inferred columns exist in the table
            for field in iceberg_schema.fields:
                column_name = field.name
                inferred_type = iceberg_type_to_trino_type(field.field_type)
                
                if column_name not in existing_columns:
                    logger.warning(f"Column {column_name} from inferred schema doesn't exist in table")
                    return False
                
                # Check type compatibility
                existing_type = existing_columns[column_name]
                if not are_types_compatible(existing_type, inferred_type):
                    logger.warning(
                        f"Column {column_name} type mismatch: existing={existing_type}, inferred={inferred_type}"
                    )
                    return False
            
            return True
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
    # If types are exactly the same, they're compatible
    if existing_type.upper() == inferred_type.upper():
        return True
    
    # Define type compatibility rules
    compatible_types = {
        'INTEGER': ['BIGINT', 'DOUBLE', 'DECIMAL'],
        'BIGINT': ['DOUBLE', 'DECIMAL'],
        'REAL': ['DOUBLE'],
        'VARCHAR': ['CHAR'],
        'CHAR': ['VARCHAR'],
    }
    
    # Check if inferred type is in the list of compatible types for the existing type
    existing_type_upper = existing_type.upper()
    if existing_type_upper in compatible_types:
        return inferred_type.upper() in compatible_types[existing_type_upper]
    
    return False
