"""
Iceberg REST client for S3 Tables integration
"""
import requests
import json
import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import datetime
import hashlib

logger = logging.getLogger(__name__)

class IcebergRestClient:
    """Client for interacting with Iceberg REST Catalog API"""
    
    SERVICE = 's3tables'
    BASE_API_PATH = '/v1'  # Base API path without /iceberg prefix
    
    def __init__(self, endpoint: str, region: str, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None, table_bucket_arn: Optional[str] = None):
        """Initialize the Iceberg REST client.
        
        Args:
            endpoint: Base URL of the Iceberg REST Catalog API
            region: AWS region
            aws_access_key_id: Optional AWS access key ID
            aws_secret_access_key: Optional AWS secret access key
            table_bucket_arn: The ARN of the S3 Tables bucket
        """
        # Parse and normalize the endpoint
        parsed = urlparse(endpoint if endpoint.startswith(('http://', 'https://')) else f'https://{endpoint}')
        self.host = parsed.netloc
        self.scheme = parsed.scheme
        self.endpoint = f"{self.scheme}://{self.host}"
        
        # Extract the base path from the endpoint if it exists
        self.base_path = parsed.path.rstrip('/') if parsed.path else ''
        
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.table_bucket_arn = table_bucket_arn
        self.session = requests.Session()
        
        # Set up AWS session
        if aws_access_key_id and aws_secret_access_key:
            logger.debug(f"Using provided AWS credentials for region {region}")
            self.aws_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region
            )
        else:
            # Try to get credentials from environment or IAM role
            logger.debug(f"No AWS credentials provided, trying environment/IAM role for region {region}")
            self.aws_session = boto3.Session(region_name=region)
        
        # Get credentials from the session
        self.credentials = self.aws_session.get_credentials()
        if not self.credentials:
            raise ValueError("No AWS credentials available")
        
        logger.debug(f"Initialized Iceberg REST client for endpoint: {self.endpoint}")
        logger.debug(f"Using base path: {self.base_path}")
        logger.debug(f"Using AWS credentials: {self.credentials.access_key}")
        logger.debug(f"AWS Region: {self.region}")
        logger.debug(f"AWS Service: {self.SERVICE}")
        logger.debug(f"Table Bucket ARN: {self.table_bucket_arn}")
    
    def _join_path(self, base: str, path: str) -> str:
        """Helper to safely join base and path without duplicate slashes."""
        return f"{base.rstrip('/')}/{path.lstrip('/')}"
    
    def _make_request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make a request to the Iceberg REST API with AWS SigV4 authentication.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response object
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        # Refresh credentials
        credentials = self.aws_session.get_credentials()
        if not credentials:
            raise ValueError("AWS credentials not available")
        frozen_credentials = credentials.get_frozen_credentials()
        
        logger.error(f"AWS Credentials: {frozen_credentials.access_key}")
        logger.error(f"AWS Region: {self.region}")
        logger.error(f"AWS Service: {self.SERVICE}")

        # Prepare request body
        body = b''
        if method in ('POST', 'PUT', 'DELETE') and 'json' in kwargs:
            body = json.dumps(kwargs['json']).encode('utf-8')

        # Construct the full URL
        url = f"{self.endpoint}/iceberg/v1/{path.lstrip('/')}"
        logger.error(f"Request URL: {url}")

        # Prepare headers
        amz_date = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        content_sha256 = hashlib.sha256(body).hexdigest()
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Host': self.host,
            'X-Amz-Date': amz_date,
            'X-Amz-Content-Sha256': content_sha256,
        }

        # Add bucket ARN if available
        if self.table_bucket_arn:
            headers['X-Amz-Bucket-Arn'] = self.table_bucket_arn

        logger.error(f"Request headers before signing: {headers}")

        # Create and sign AWS request
        aws_request = AWSRequest(
            method=method,
            url=url,
            headers=headers,
            data=body
        )
        
        # Sign the request
        signer = SigV4Auth(frozen_credentials, self.SERVICE, self.region)
        signer.add_auth(aws_request)
        
        # Debug the signed request
        aws_prepared = aws_request.prepare()
        logger.error(f"Authorization header: {aws_prepared.headers.get('Authorization', 'Not found')}")
        
        # Convert to requests.PreparedRequest
        prepared_request = requests.Request(
            method=method,
            url=url,
            headers=dict(aws_prepared.headers),
            data=body
        ).prepare()
        
        try:
            # Send the request using the session
            response = self.session.send(prepared_request)
            
            # Log response details for debugging
            if response.status_code >= 400:
                logger.error(f"Request failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response text: {e.response.text}")
            raise
    
    def list_tables(self, namespace: str) -> List[str]:
        """List tables in a namespace.
        
        Args:
            namespace: Namespace to list tables from
            
        Returns:
            List of table names
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"=== Starting list_tables for namespace {namespace} ===")
        logger.debug(f"Using endpoint: {self.endpoint}")
        logger.debug(f"Using region: {self.region}")
        logger.debug(f"Using credentials: {self.credentials.access_key if self.credentials else 'None'}")
        logger.debug(f"Using bucket ARN: {self.table_bucket_arn}")
        
        response = self._make_request("GET", f"/namespaces/{namespace}/tables")
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
        response = self._make_request("GET", f"/namespaces/{namespace}/tables/{table_name}")
        return response.json()
    
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
        response = self._make_request("POST", f"/namespaces/{namespace}/tables", json=payload)
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
        response = self._make_request("POST", f"/namespaces/{namespace}/tables/{table_name}/update", 
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
        self._make_request("DELETE", f"/namespaces/{namespace}/tables/{table_name}")
    
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
        response = self._make_request("POST", f"/namespaces/{namespace}/tables/{table_name}/commit",
                                    json={"operations": operations})
        return response.json() 