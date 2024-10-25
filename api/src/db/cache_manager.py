import os
import threading
import duckdb
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class CacheManager:
    """
    Encapsulate all caching logic making it easier to manage and extend.
    Use a lock to ensure thread safety when accessing or updating the cache.
    """
    def __init__(self):
        self._cached_data = None
        self._cached_data_last_modified = None
        self._lock = threading.Lock()

    def _get_boto3_session(self):
        """
        Create a boto3 session that can automatically refresh temporary credentials.
        Uses environment variables for access key and secret key if present.
        """
        try:
            session = boto3.Session(
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                region_name=os.environ.get('AWS_REGION', 'us-east-1')
            )
            return session
        except (NoCredentialsError, PartialCredentialsError) as exp:
            raise RuntimeError("AWS credentials are not configured properly.") from exp

    def load_data_into_cache(self):
        """
        Load the data from S3 into the cache if it has been modified.
        """
        with self._lock:
            session = self._get_boto3_session()
            s3_client = session.client('s3')

            s3_bucket = os.environ.get('S3_BUCKET_NAME')
            s3_key = os.environ.get('S3_PARQUET_KEY', 'all_infras_final.parquet')

            # Get the last modified time of the S3 object
            response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            last_modified = response['LastModified']

            # Check if the data has been modified
            if self._cached_data is None or last_modified != self._cached_data_last_modified:
                conn = duckdb.connect(database=':memory:')
                conn.execute("INSTALL httpfs;")
                conn.execute("LOAD httpfs;")
                conn.execute(f"SET s3_region='{session.region_name}';")
                conn.execute(f"SET s3_access_key_id='{session.get_credentials().access_key}';")
                conn.execute(f"SET s3_secret_access_key='{session.get_credentials().secret_key}';")

                aws_session_token = session.get_credentials().token
                if aws_session_token:
                    conn.execute(f"SET s3_session_token='{aws_session_token}';")

                s3_uri = f"s3://{s3_bucket}/{s3_key}"
                df = conn.execute(f"SELECT * FROM '{s3_uri}'").fetchdf()

                # Store the DataFrame in the cache and update the last modified time
                self._cached_data = df
                self._cached_data_last_modified = last_modified

                conn.close()

    def get_cached_data(self):
        """
        Get the cached DataFrame. If the cache is empty, it will load the data.
        """
        with self._lock:
            if self._cached_data is None:
                self.load_data_into_cache()
            return self._cached_data

    def create_duckdb_connection(self):
        """
        Create a DuckDB connection with the cached data registered.
        """
        conn = duckdb.connect(database=':memory:')
        conn.register('all_infras', self.get_cached_data())
        return conn
