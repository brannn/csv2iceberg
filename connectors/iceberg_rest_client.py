"""
Iceberg REST client for S3 Tables integration
"""
import requests
import json
import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class IcebergRestClient:
    """Client for interacting with Iceberg REST Catalog API"""
    
    def __init__(self, endpoint: str, region: str, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None):
        """Initialize the Iceberg REST client.
        
        Args:
            endpoint: Base URL of the Iceberg REST Catalog API
            region: AWS region
            aws_access_key_id: Optional AWS access key ID
            aws_secret_access_key: Optional AWS secret access key
        """
        self.endpoint = endpoint.rstrip('/')
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.session = requests.Session()
        
        # Set up AWS authentication if credentials are provided
        if aws_access_key_id and aws_secret_access_key:
            self.session.auth = (aws_access_key_id, aws_secret_access_key)
        
        logger.debug(f"Initialized Iceberg REST client for endpoint: {endpoint}")
    
    def _make_request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make a request to the Iceberg REST API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response object
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = urljoin(self.endpoint, path)
        logger.debug(f"Making {method} request to {url}")
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    def create_table(self, namespace: str, table_name: str, schema: Dict[str, Any], 
                    partition_spec: Optional[List[Dict[str, Any]]] = None,
                    location: Optional[str] = None) -> Dict[str, Any]:
        """Create a new Iceberg table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            schema: Table schema
            partition_spec: Optional partition specification
            location: Optional table location
            
        Returns:
            Table metadata
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        payload = {
            "name": f"{namespace}.{table_name}",
            "schema": schema,
            "properties": {
                "format-version": "2"
            }
        }
        
        if partition_spec:
            payload["partition-spec"] = partition_spec
            
        if location:
            payload["location"] = location
            
        logger.debug(f"Creating table {namespace}.{table_name}")
        response = self._make_request("POST", "/v1/tables", json=payload)
        return response.json()
    
    def list_tables(self, namespace: str) -> List[str]:
        """List tables in a namespace.
        
        Args:
            namespace: Namespace to list tables from
            
        Returns:
            List of table names
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Listing tables in namespace {namespace}")
        response = self._make_request("GET", f"/v1/namespaces/{namespace}/tables")
        return response.json()
    
    def get_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            Table metadata
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Getting metadata for table {namespace}.{table_name}")
        response = self._make_request("GET", f"/v1/tables/{namespace}/{table_name}")
        return response.json()
    
    def update_table(self, namespace: str, table_name: str, 
                    updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update table metadata.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            updates: Table updates
            
        Returns:
            Updated table metadata
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Updating table {namespace}.{table_name}")
        response = self._make_request("POST", f"/v1/tables/{namespace}/{table_name}/update", 
                                    json=updates)
        return response.json()
    
    def delete_table(self, namespace: str, table_name: str) -> None:
        """Delete a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Deleting table {namespace}.{table_name}")
        self._make_request("DELETE", f"/v1/tables/{namespace}/{table_name}")
    
    def commit(self, namespace: str, table_name: str, 
              operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Commit changes to a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            operations: List of operations to commit
            
        Returns:
            Commit result
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Committing changes to table {namespace}.{table_name}")
        response = self._make_request("POST", f"/v1/tables/{namespace}/{table_name}/commit",
                                    json={"operations": operations})
        return response.json() 