"""
Unit tests for the Trino adapter.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

# Mark all tests in this file as using trino-specific functionality
pytestmark = [
    pytest.mark.db,
    pytest.mark.trino
]

from sql_batcher.adapters.trino import TrinoAdapter


class TestTrinoAdapter:
    """Test cases for TrinoAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch):
        """Set up test fixtures."""
        # Mock the trino.dbapi module
        self.mock_trino = MagicMock()
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        
        # Configure the mocks
        self.mock_connection.cursor.return_value = self.mock_cursor
        self.mock_trino.connect.return_value = self.mock_connection
        
        # Patch the trino.dbapi module
        monkeypatch.setattr('sql_batcher.adapters.trino.trino.dbapi', self.mock_trino)
        
        # Create the adapter
        self.adapter = TrinoAdapter(
            host='localhost',
            port=8080,
            user='test_user',
            catalog='test_catalog',
            schema='test_schema'
        )
        
        # Set up mock session properties for testing
        self.adapter._session_properties = {
            'query_max_run_time': '2h',
            'distributed_join': 'true'
        }

    def test_init(self):
        """Test initialization."""
        # Check that the connection was created with the correct parameters
        self.mock_trino.connect.assert_called_once()
        call_kwargs = self.mock_trino.connect.call_args.kwargs
        
        assert call_kwargs['host'] == 'localhost'
        assert call_kwargs['port'] == 8080
        assert call_kwargs['user'] == 'test_user'
        assert call_kwargs['catalog'] == 'test_catalog'
        assert call_kwargs['schema'] == 'test_schema'
        
        # Check that the adapter has the correct properties
        assert self.adapter._connection == self.mock_connection
        assert self.adapter._cursor == self.mock_cursor

    def test_get_max_query_size(self):
        """Test get_max_query_size method."""
        # Trino has a default max query size of 1MB (1,000,000 bytes)
        assert self.adapter.get_max_query_size() == 1_000_000

    def test_execute_select(self):
        """Test executing a SELECT statement."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [('id',), ('name',)]
        self.mock_cursor.fetchall.return_value = [(1, 'Test User'), (2, 'Another User')]
        
        # Execute a SELECT statement
        result = self.adapter.execute("SELECT id, name FROM users")
        
        # Verify the query was executed with the correct SQL
        self.mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")
        
        # Verify the result contains the expected data
        assert result == [(1, 'Test User'), (2, 'Another User')]

    def test_execute_insert(self):
        """Test executing an INSERT statement."""
        # Configure the mock cursor for an INSERT
        self.mock_cursor.description = None
        self.mock_cursor.rowcount = 1
        
        # Execute an INSERT statement
        result = self.adapter.execute("INSERT INTO users VALUES (3, 'New User')")
        
        # Verify the query was executed with the correct SQL
        self.mock_cursor.execute.assert_called_once_with("INSERT INTO users VALUES (3, 'New User')")
        
        # Verify the result is empty for non-SELECT statements
        assert result == []

    def test_execute_with_session_properties(self):
        """Test execution with session properties."""
        # Configure the mock cursor
        self.mock_cursor.description = None
        
        # Execute a statement
        self.adapter.execute("CREATE TABLE test (id INT, name VARCHAR)")
        
        # Verify session properties were set
        assert self.mock_cursor.execute.call_count == 3
        
        # First call should set query_max_run_time
        self.mock_cursor.execute.assert_any_call("SET SESSION query_max_run_time = '2h'")
        
        # Second call should set distributed_join
        self.mock_cursor.execute.assert_any_call("SET SESSION distributed_join = 'true'")
        
        # Third call should execute the actual statement
        self.mock_cursor.execute.assert_any_call("CREATE TABLE test (id INT, name VARCHAR)")

    def test_begin_transaction(self):
        """Test beginning a transaction."""
        # Run the method
        self.adapter.begin_transaction()
        
        # Verify a start transaction statement was executed
        self.mock_cursor.execute.assert_called_once_with("START TRANSACTION")

    def test_commit_transaction(self):
        """Test committing a transaction."""
        # Run the method
        self.adapter.commit_transaction()
        
        # Verify a commit statement was executed
        self.mock_cursor.execute.assert_called_once_with("COMMIT")

    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        # Run the method
        self.adapter.rollback_transaction()
        
        # Verify a rollback statement was executed
        self.mock_cursor.execute.assert_called_once_with("ROLLBACK")

    def test_close(self):
        """Test closing the connection."""
        # Run the method
        self.adapter.close()
        
        # Verify the cursor and connection were closed
        self.mock_cursor.close.assert_called_once()
        self.mock_connection.close.assert_called_once()

    def test_get_catalogs(self):
        """Test getting available catalogs."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [('catalog_name',)]
        self.mock_cursor.fetchall.return_value = [('hive',), ('mysql',), ('postgres',)]
        
        # Get the catalogs
        result = self.adapter.get_catalogs()
        
        # Verify the correct query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")
        
        # Verify the result
        assert result == ['hive', 'mysql', 'postgres']

    def test_get_schemas(self):
        """Test getting available schemas in a catalog."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [('schema_name',)]
        self.mock_cursor.fetchall.return_value = [('default',), ('public',), ('information_schema',)]
        
        # Get the schemas
        result = self.adapter.get_schemas('hive')
        
        # Verify the correct query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS FROM hive")
        
        # Verify the result
        assert result == ['default', 'public', 'information_schema']

    def test_get_tables(self):
        """Test getting available tables in a schema."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [('table_name',)]
        self.mock_cursor.fetchall.return_value = [('users',), ('orders',), ('products',)]
        
        # Get the tables
        result = self.adapter.get_tables('hive', 'default')
        
        # Verify the correct query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW TABLES FROM hive.default")
        
        # Verify the result
        assert result == ['users', 'orders', 'products']

    def test_get_columns(self):
        """Test getting columns for a table."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [('column_name', 'type')]
        self.mock_cursor.fetchall.return_value = [
            ('id', 'integer'), 
            ('name', 'varchar'), 
            ('created_at', 'timestamp')
        ]
        
        # Get the columns
        result = self.adapter.get_columns('users', 'hive', 'default')
        
        # Verify the correct query was executed
        self.mock_cursor.execute.assert_called_once_with(
            "SELECT column_name, data_type FROM hive.information_schema.columns "
            "WHERE table_schema = 'default' AND table_name = 'users'"
        )
        
        # Verify the result
        assert result == [
            {'name': 'id', 'type': 'integer'}, 
            {'name': 'name', 'type': 'varchar'}, 
            {'name': 'created_at', 'type': 'timestamp'}
        ]

    def test_set_session_property(self):
        """Test setting a session property."""
        # Clear previous calls
        self.mock_cursor.reset_mock()
        
        # Set a session property
        self.adapter.set_session_property('join_distribution_type', 'PARTITIONED')
        
        # Verify the property was set
        self.mock_cursor.execute.assert_called_once_with("SET SESSION join_distribution_type = 'PARTITIONED'")
        
        # Verify it was added to the stored properties
        assert self.adapter._session_properties['join_distribution_type'] == 'PARTITIONED'

    def test_execute_with_http_headers(self):
        """Test execution with HTTP headers."""
        # Create an adapter with HTTP headers
        adapter_with_headers = TrinoAdapter(
            host='localhost',
            port=8080,
            user='test_user',
            catalog='test_catalog',
            schema='test_schema',
            http_headers={'X-Trino-Client-Info': 'sql-batcher-test'}
        )
        
        # Verify the connection was created with the HTTP headers
        call_kwargs = self.mock_trino.connect.call_args_list[1].kwargs
        assert call_kwargs['http_headers'] == {'X-Trino-Client-Info': 'sql-batcher-test'}

    def test_missing_trino_package(self, monkeypatch):
        """Test behavior when trino package is not installed."""
        # Simulate the trino package not being installed
        monkeypatch.setattr('sql_batcher.adapters.trino.trino', None)
        
        # Attempting to create the adapter should raise an ImportError
        with pytest.raises(ImportError) as excinfo:
            TrinoAdapter(
                host='localhost',
                port=8080,
                user='test_user',
                catalog='test_catalog',
                schema='test_schema'
            )
        
        # Verify the error message
        assert "trino package is required" in str(excinfo.value)