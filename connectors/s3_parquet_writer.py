"""
S3 Parquet writer for S3 Tables integration
"""
import os
import logging
import tempfile
from typing import Dict, Any, Optional, List
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3ParquetWriter:
    """Writer for Parquet files to S3"""
    
    def __init__(self, region: str, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None):
        """Initialize the S3 Parquet writer.
        
        Args:
            region: AWS region
            aws_access_key_id: Optional AWS access key ID
            aws_secret_access_key: Optional AWS secret access key
        """
        self.region = region
        self.s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        logger.debug(f"Initialized S3 Parquet writer for region: {region}")
    
    def file_exists(self, bucket: str, key: str) -> bool:
        """Check if a file exists in S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            True if the file exists, False otherwise
            
        Raises:
            ClientError: If the S3 operation fails for reasons other than file not found
        """
        try:
            logger.debug(f"Checking if file exists: s3://{bucket}/{key}")
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            logger.error(f"Error checking file existence: {str(e)}")
            raise
    
    def write_dataframe(self, df: pa.Table, bucket: str, key: str,
                       compression: str = 'snappy') -> str:
        """Write a PyArrow table to S3 as a Parquet file.
        
        Args:
            df: PyArrow table to write
            bucket: S3 bucket name
            key: S3 object key
            compression: Parquet compression codec
            
        Returns:
            S3 URI of the written file
            
        Raises:
            ClientError: If the S3 operation fails
        """
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Write the table to the temporary file
            logger.debug(f"Writing Parquet file to temporary location: {temp_path}")
            pq.write_table(df, temp_path, compression=compression)
            
            # Upload to S3
            logger.debug(f"Uploading Parquet file to s3://{bucket}/{key}")
            self.s3_client.upload_file(temp_path, bucket, key)
            
            # Return the S3 URI
            return f"s3://{bucket}/{key}"
            
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_path)
            except OSError:
                logger.warning(f"Could not remove temporary file: {temp_path}")
    
    def write_dataframe_multipart(self, df: pa.Table, bucket: str, key: str,
                                compression: str = 'snappy',
                                chunk_size: int = 1000000) -> str:
        """Write a large PyArrow table to S3 as a Parquet file using multipart upload.
        
        Args:
            df: PyArrow table to write
            bucket: S3 bucket name
            key: S3 object key
            compression: Parquet compression codec
            chunk_size: Number of rows per chunk
            
        Returns:
            S3 URI of the written file
            
        Raises:
            ClientError: If the S3 operation fails
        """
        # Create a temporary directory for chunks
        with tempfile.TemporaryDirectory() as temp_dir:
            # Split the table into chunks
            num_rows = len(df)
            num_chunks = (num_rows + chunk_size - 1) // chunk_size
            
            # Create multipart upload
            logger.debug(f"Starting multipart upload to s3://{bucket}/{key}")
            mpu = self.s3_client.create_multipart_upload(
                Bucket=bucket,
                Key=key
            )
            
            try:
                parts = []
                
                # Upload each chunk
                for i in range(num_chunks):
                    start = i * chunk_size
                    end = min((i + 1) * chunk_size, num_rows)
                    
                    # Get the chunk
                    chunk = df.slice(start, end - start)
                    
                    # Write chunk to temporary file
                    chunk_path = os.path.join(temp_dir, f"chunk_{i}.parquet")
                    pq.write_table(chunk, chunk_path, compression=compression)
                    
                    # Upload part
                    logger.debug(f"Uploading part {i+1}/{num_chunks}")
                    with open(chunk_path, 'rb') as f:
                        part = self.s3_client.upload_part(
                            Bucket=bucket,
                            Key=key,
                            PartNumber=i+1,
                            UploadId=mpu['UploadId'],
                            Body=f
                        )
                        parts.append({
                            'PartNumber': i+1,
                            'ETag': part['ETag']
                        })
                
                # Complete multipart upload
                logger.debug("Completing multipart upload")
                self.s3_client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=mpu['UploadId'],
                    MultipartUpload={'Parts': parts}
                )
                
                # Return the S3 URI
                return f"s3://{bucket}/{key}"
                
            except Exception as e:
                # Abort multipart upload on failure
                logger.error(f"Multipart upload failed: {str(e)}")
                self.s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=mpu['UploadId']
                )
                raise
    
    def delete_file(self, bucket: str, key: str) -> None:
        """Delete a file from S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Raises:
            ClientError: If the S3 operation fails
        """
        logger.debug(f"Deleting file s3://{bucket}/{key}")
        self.s3_client.delete_object(Bucket=bucket, Key=key)
    
    def list_files(self, bucket: str, prefix: str) -> List[str]:
        """List files in an S3 bucket with a given prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: Key prefix
            
        Returns:
            List of S3 URIs
            
        Raises:
            ClientError: If the S3 operation fails
        """
        logger.debug(f"Listing files in s3://{bucket}/{prefix}")
        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return []
            
        return [f"s3://{bucket}/{obj['Key']}" for obj in response['Contents']] 