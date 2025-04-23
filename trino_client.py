"""
Trino client module for CSV to Iceberg conversion
"""
import logging
from typing import Dict, List, Any, Optional, Tuple

# Import our mock Schema class since we're not using the real PyIceberg
from schema_inferrer import Schema, Field

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
        http_scheme: str = 'http'
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
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        
        self.connection = self._create_connection()
        
    def _create_connection(self):
        """Create a connection to Trino"""
        try:
            # For CLI demo purposes, just log the connection info
            # In a real environment, this would establish the actual connection
            auth_msg = "with authentication" if self.password else "without authentication"
            logger.info(f"Would connect to Trino at {self.host}:{self.port} as user '{self.user}' {auth_msg}")
            return None
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}", exc_info=True)
            raise ConnectionError(f"Could not connect to Trino: {str(e)}")
    
    def execute_query(self, query: str) -> List[Tuple]:
        """
        Execute a query on Trino.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result rows
        """
        # For CLI demo purposes, just log the query
        # In a real environment, this would execute the query
        logger.info(f"Would execute query: {query}")
        return []
    
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
        # For CLI demo purposes, always return False (table doesn't exist)
        logger.info(f"Checking if table exists: {catalog}.{schema}.{table}")
        return False
    
    def create_iceberg_table(
        self, 
        catalog: str, 
        schema: str, 
        table: str, 
        iceberg_schema: Schema,
        partition_spec: Optional[List[str]] = None  # Kept for backward compatibility but not used
    ) -> None:
        """
        Create an Iceberg table using the provided schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            iceberg_schema: PyIceberg Schema object
            partition_spec: Parameter kept for backward compatibility but not used
        """
        try:
            # Convert PyIceberg schema to Trino DDL
            columns_ddl = []
            for field in iceberg_schema.fields:
                column_name = field.name
                column_type = iceberg_type_to_trino_type(field.field_type)
                columns_ddl.append(f"{column_name} {column_type}")
            
            columns_clause = ", ".join(columns_ddl)
            
            # Start building the CREATE TABLE SQL
            create_table_sql = f"""
            CREATE TABLE {catalog}.{schema}.{table} (
                {columns_clause}
            )"""
            
            # Add Iceberg table format
            create_table_sql += """
            WITH (
                format = 'ICEBERG'
            )
            """
            
            logger.info(f"Creating Iceberg table: {catalog}.{schema}.{table}")
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
            existing_columns = {col_name: col_type for col_name, col_type in existing_schema}
            
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
    # Import our mock types
    from schema_inferrer import (
        BooleanType, IntegerType, LongType, FloatType, DoubleType, 
        DateType, TimestampType, StringType, DecimalType, StructType
    )
    
    # For this demo implementation, we'll return default SQL types
    # based on the instance type
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
        return 'TIMESTAMP'
    elif isinstance(iceberg_type, StringType):
        return 'VARCHAR'
    elif isinstance(iceberg_type, DecimalType):
        precision = getattr(iceberg_type, 'precision', 38)
        scale = getattr(iceberg_type, 'scale', 18)
        return f'DECIMAL({precision}, {scale})'
    elif isinstance(iceberg_type, StructType):
        # Since this is a mock implementation, we'll just return a placeholder
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
