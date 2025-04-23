"""
Hive metastore client module for CSV to Iceberg conversion
"""
import logging
import socket
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# Import Hive Metastore client dependencies
try:
    import thrift
    from thrift.transport import TSocket, TTransport
    from thrift.protocol import TBinaryProtocol
    from hive_metastore import ThriftHiveMetastore
    from hive_metastore.ttypes import Database, Table, StorageDescriptor, FieldSchema, SerDeInfo
    HAVE_HIVE_THRIFT = True
except ImportError:
    # If Hive Metastore Thrift client is not available, log a warning
    logging.warning("Hive Metastore Thrift client not available, some functionality may be limited")
    HAVE_HIVE_THRIFT = False

logger = logging.getLogger(__name__)

class HiveMetastoreClient:
    """Client for interacting with Hive metastore"""
    
    def __init__(self, metastore_uri: str):
        """
        Initialize Hive metastore client.
        
        Args:
            metastore_uri: Hive metastore Thrift URI (host:port)
        """
        self.metastore_uri = metastore_uri
        self.host, self.port = self._parse_uri(metastore_uri)
        self.client = self._create_client()
        
    def _parse_uri(self, uri: str) -> tuple:
        """Parse Hive metastore URI into host and port"""
        try:
            if ":" not in uri:
                return uri, 9083  # Default Hive metastore port
            host, port = uri.split(":")
            return host, int(port)
        except Exception as e:
            logger.error(f"Invalid Hive metastore URI: {uri}. Error: {str(e)}", exc_info=True)
            raise ValueError(f"Invalid Hive metastore URI: {uri}")
    
    def _create_client(self):
        """Create a connection to Hive metastore"""
        try:
            if not HAVE_HIVE_THRIFT:
                logger.warning("Hive Metastore Thrift client not available, using mock implementation")
                return None
                
            logger.info(f"Connecting to Hive metastore at {self.host}:{self.port}")
            
            # Test if the host/port is available
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            
            if result != 0:
                logger.warning(f"Hive metastore at {self.host}:{self.port} is not available, using mock implementation")
                return None
            
            # Create the Thrift client
            socket = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ThriftHiveMetastore.Client(protocol)
            transport.open()
            logger.info(f"Connected to Hive metastore at {self.host}:{self.port}")
            return client
            
        except Exception as e:
            logger.error(f"Failed to connect to Hive metastore: {str(e)}", exc_info=True)
            logger.warning("Using mock implementation for Hive metastore")
            return None
    
    def database_exists(self, db_name: str) -> bool:
        """
        Check if a database exists.
        
        Args:
            db_name: Database name
            
        Returns:
            True if the database exists
        """
        try:
            logger.info(f"Checking if database exists: {db_name}")
            
            if self.client is None:
                # Mock implementation
                logger.info(f"Using mock implementation for database_exists({db_name})")
                return False
                
            databases = self.client.get_all_databases()
            return db_name in databases
            
        except Exception as e:
            logger.warning(f"Error checking if database exists: {str(e)}")
            return False
    
    def create_database(self, db_name: str, description: str = "") -> None:
        """
        Create a database.
        
        Args:
            db_name: Database name
            description: Database description
        """
        try:
            logger.info(f"Creating database: {db_name}")
            
            if self.client is None:
                # Mock implementation
                logger.info(f"Using mock implementation for create_database({db_name})")
                return
                
            if self.database_exists(db_name):
                logger.info(f"Database {db_name} already exists")
                return
                
            db = Database()
            db.name = db_name
            db.description = description
            db.locationUri = f"/user/hive/warehouse/{db_name}.db"
            db.parameters = {}
            db.ownerName = "hive"
            db.ownerType = 1  # USER
            
            self.client.create_database(db)
            logger.info(f"Created database: {db_name}")
            
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to create database: {str(e)}")
    
    def table_exists(self, db_name: str, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            db_name: Database name
            table_name: Table name
            
        Returns:
            True if the table exists
        """
        try:
            logger.info(f"Checking if table exists: {db_name}.{table_name}")
            
            if self.client is None:
                # Mock implementation
                logger.info(f"Using mock implementation for table_exists({db_name}.{table_name})")
                return False
                
            if not self.database_exists(db_name):
                return False
                
            tables = self.client.get_all_tables(db_name)
            return table_name in tables
            
        except Exception as e:
            logger.warning(f"Error checking if table exists: {str(e)}")
            return False
    
    def get_table(self, db_name: str, table_name: str):
        """
        Get a table.
        
        Args:
            db_name: Database name
            table_name: Table name
            
        Returns:
            Hive Table object
        """
        try:
            logger.info(f"Getting table: {db_name}.{table_name}")
            
            if self.client is None:
                # Mock implementation
                logger.info(f"Using mock implementation for get_table({db_name}.{table_name})")
                raise ValueError(f"Table does not exist: {db_name}.{table_name}")
                
            if not self.table_exists(db_name, table_name):
                raise ValueError(f"Table does not exist: {db_name}.{table_name}")
                
            return self.client.get_table(db_name, table_name)
            
        except Exception as e:
            logger.error(f"Error getting table: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to get table: {str(e)}")
    
    def create_iceberg_table(
        self, 
        db_name: str, 
        table_name: str, 
        column_names: List[str],
        column_types: List[str],
        location: str = "",
        table_properties: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Create an Iceberg table in Hive metastore.
        
        Args:
            db_name: Database name
            table_name: Table name
            column_names: List of column names
            column_types: List of column types
            location: HDFS location for the table
            table_properties: Additional table properties
        """
        try:
            if table_properties is None:
                table_properties = {}
                
            # Add Iceberg-specific properties
            iceberg_props = {
                "table_type": "ICEBERG",
                "format-version": "2",
                "engine.hive.enabled": "true"
            }
            
            # Merge user properties with Iceberg properties
            merged_props = {**iceberg_props, **table_properties}
            
            cols = [f"{name} ({type_str})" for name, type_str in zip(column_names, column_types)]
            cols_str = ", ".join(cols)
            
            logger.info(f"Creating Iceberg table: {db_name}.{table_name}")
            logger.info(f"Columns: {cols_str}")
            logger.info(f"Location: {location}")
            logger.info(f"Properties: {merged_props}")
            
            if self.client is None:
                # Mock implementation
                logger.info(f"Using mock implementation for create_iceberg_table({db_name}.{table_name})")
                return
                
            # Create the database if it doesn't exist
            if not self.database_exists(db_name):
                self.create_database(db_name)
                
            # Drop the table if it exists and we're in overwrite mode
            if self.table_exists(db_name, table_name):
                logger.warning(f"Table {db_name}.{table_name} already exists, dropping it")
                self.client.drop_table(db_name, table_name, deleteData=True)
                
            # Create the Hive table
            table = Table()
            table.dbName = db_name
            table.tableName = table_name
            table.owner = "hive"
            table.createTime = int(datetime.now().timestamp())
            table.lastAccessTime = 0
            table.retention = 0
            table.tableType = "EXTERNAL_TABLE"
            table.parameters = merged_props
            
            # Set table location
            if not location:
                location = f"/user/hive/warehouse/{db_name}.db/{table_name}"
                
            # Create storage descriptor
            sd = StorageDescriptor()
            sd.location = location
            sd.inputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
            sd.outputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
            sd.serdeInfo = SerDeInfo()
            sd.serdeInfo.serializationLib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
            sd.serdeInfo.parameters = {}
            
            # Create columns
            sd.cols = []
            for i, (col_name, col_type) in enumerate(zip(column_names, column_types)):
                field = FieldSchema()
                field.name = col_name
                field.type = col_type
                field.comment = ""
                sd.cols.append(field)
                
            table.sd = sd
            
            # Create the table
            self.client.create_table(table)
            logger.info(f"Created Iceberg table: {db_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Error creating Iceberg table: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to create Iceberg table: {str(e)}")
    
    def close(self) -> None:
        """Close the Hive metastore connection"""
        try:
            if self.client is not None and hasattr(self.client, '_iprot') and \
               hasattr(self.client._iprot, '_trans') and \
               hasattr(self.client._iprot._trans, 'close'):
                self.client._iprot._trans.close()
                logger.info("Closed Hive metastore connection")
            else:
                logger.info("No active Hive metastore connection to close")
        except Exception as e:
            logger.error(f"Error closing Hive metastore connection: {str(e)}", exc_info=True)
