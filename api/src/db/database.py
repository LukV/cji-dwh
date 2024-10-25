import os
import threading
import duckdb
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Global variables to hold the cached data and a lock for thread safety
_cached_data = None
_cached_data_lock = threading.Lock()
_cached_data_last_modified = None

def get_boto3_session():
    """
    Create a boto3 session that can automatically refresh temporary credentials.
    Uses environment variables for access key and secret key if present.
    
    Returns:
        boto3.Session: A session object to interact with AWS services.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_REGION')
        )
        return session
    except (NoCredentialsError, PartialCredentialsError):
        raise RuntimeError("AWS credentials are not configured properly.")

def load_data_into_cache():
    """
    Load the data from S3 into the global cache if it has been modified.
    """
    global _cached_data, _cached_data_last_modified
    with _cached_data_lock:
        # Create a boto3 session and client
        session = get_boto3_session()
        s3_client = session.client('s3')

        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        s3_key = os.environ.get('S3_PARQUET_KEY')

        # Get the last modified time of the S3 object
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        last_modified = response['LastModified']

        # Check if the data has been modified
        if _cached_data is None or last_modified != _cached_data_last_modified:
            # Proceed to load data
            conn = duckdb.connect(database=':memory:')
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
            conn.execute(f"SET s3_region='{session.region_name}';")
            conn.execute(f"SET s3_access_key_id='{session.get_credentials().access_key}';")
            conn.execute(f"SET s3_secret_access_key='{session.get_credentials().secret_key}';")

            # Get the session token if available (for temporary credentials)
            aws_session_token = session.get_credentials().token
            if aws_session_token:
                conn.execute(f"SET s3_session_token='{aws_session_token}';")

            # Construct the S3 URI
            s3_uri = f"s3://{s3_bucket}/{s3_key}"

            # Read the Parquet file from S3 into a DataFrame
            df = conn.execute(f"SELECT * FROM '{s3_uri}'").fetchdf()

            # Store the DataFrame in the global cache and update the last modified time
            _cached_data = df
            _cached_data_last_modified = last_modified

            # Close the connection
            conn.close()


def get_db_connection():
    """
    Get a DuckDB connection with the cached data registered.

    Returns:
        duckdb.DuckDBPyConnection: A connection object to interact with the cached data.
    """
    global _cached_data
    with _cached_data_lock:
        if _cached_data is None:
            load_data_into_cache()

    # Create a new in-memory DuckDB connection
    conn = duckdb.connect(database=':memory:')
    # Register the cached DataFrame as a table
    conn.register('all_infras', _cached_data)
    return conn
