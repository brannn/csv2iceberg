import requests
import datetime
import hashlib
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import time

def signed_s3tables_request(method: str, path: str, body: bytes = b'', bucket_arn: str = None, is_table_operation: bool = False):
    # Use static credentials directly
    credentials = Credentials(
        access_key='AKIAWYED7MW4OEHJV7N6',
        secret_key='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'
    )

    # Base endpoint without any path
    endpoint = "https://s3tables.us-east-1.amazonaws.com"
    host = "s3tables.us-east-1.amazonaws.com"

    # Construct the full path including /iceberg/v1/
    full_path = f"/iceberg/v1/{path.lstrip('/')}"
    url = f"{endpoint}{full_path}"

    # Get current UTC time
    amz_date = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Host': host,
        'X-Amz-Date': amz_date,
        'X-Amz-Content-Sha256': hashlib.sha256(body).hexdigest(),
    }

    # Add bucket ARN only for table-specific operations
    if bucket_arn and is_table_operation:
        # The bucket ARN should be in the format: arn:aws:s3tables:region:account-id:bucket/bucket-name
        headers['X-Amz-Bucket-Arn'] = bucket_arn

    # Create AWS request with the full path
    aws_request = AWSRequest(
        method=method,
        url=url,
        headers=headers,
        data=body
    )
    SigV4Auth(credentials, 's3tables', 'us-east-1').add_auth(aws_request)

    aws_prepared = aws_request.prepare()
    print("\nRequest details:")
    print("URL:", aws_prepared.url)
    print("\nHeaders:")
    for key, value in aws_prepared.headers.items():
        print(f"{key}: {value}")
    print("\nBody:", aws_prepared.body)

    # Convert to requests.PreparedRequest
    prepared_request = requests.Request(
        method=method,
        url=url,
        headers=dict(aws_prepared.headers),
        data=body
    ).prepare()

    return requests.Session().send(prepared_request)

if __name__ == "__main__":
    print("Testing S3Tables request...")
    # Try with and without bucket ARN
    print("\nTesting without bucket ARN:")
    resp = signed_s3tables_request("GET", "namespaces/csv/tables")
    print("\nResponse:")
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.text}")

    print("\nTesting with bucket ARN (should not be included for list operation):")
    bucket_arn = "arn:aws:s3tables:us-east-1:464133252536:bucket/milton-test"
    resp = signed_s3tables_request("GET", "namespaces/csv/tables", bucket_arn=bucket_arn, is_table_operation=False)
    print("\nResponse:")
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.text}") 