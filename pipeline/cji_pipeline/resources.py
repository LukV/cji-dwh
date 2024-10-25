from dagster import resource, Field
from dagster_duckdb import DuckDBResource
from pandas import DataFrame
from authlib.integrations.httpx_client import OAuth2Client
import boto3

@resource(
    config_schema={
        "aws_access_key_id": Field(str, is_required=True),
        "aws_secret_access_key": Field(str, is_required=True),
        "region_name": Field(str, is_required=False, default_value="eu-central-1"),
        "s3_bucket_name": Field(str, is_required=True),
    }
)
def s3_resource(context):
    """
    Dagster resource to upload to and download from an S3 bucket.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=context.resource_config["aws_access_key_id"],
        aws_secret_access_key=context.resource_config["aws_secret_access_key"],
        region_name=context.resource_config.get("region_name", "eu-central-1"),
    )
    s3_client.bucket_name = context.resource_config["s3_bucket_name"]
    return s3_client

@resource
def duckdb_resource():
    """
    Provides an in-memory DuckDB resource for data processing.
    """
    return DuckDBResource(database=":memory:")

class LinkedDataAPI:
    """
    LinkedDataAPI is a class that provides methods to interact 
    with the Linked Data API of UiTwisselingsplatform.
    """
    def __init__(self, client_id, client_secret, token_url, data_endpoint, query):
        # Initialize OAuth2Session without client_secret
        self.client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            scope='profile email openid')

        self.client.fetch_token(token_url)

        self.data_endpoint = data_endpoint
        self.query = query

    def fetch_data(self):
        """Fetch data from the Linked Data API."""
        response = self.client.post(self.data_endpoint, data={'query': self.query}, timeout=None)

        if response.status_code == 200:
            data = response.json()
            df = DataFrame(data['results']['bindings'])
            # Process DataFrame to extract the 'value' fields
            return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)

        print(f'Error: {response.status_code} - {response.text}')
        return None

@resource(config_schema={
    "client_id": Field(str, is_required=True),
    "client_secret": Field(str, is_required=True),
    "token_url": Field(str, is_required=True),
    "data_endpoint": Field(str, is_required=True),
    "query": Field(str, is_required=True)
})
def linked_data_api_resource(context):
    """Dagster resource that provides the LinkedDataAPI."""
    return LinkedDataAPI(
        client_id=context.resource_config["client_id"],
        client_secret=context.resource_config["client_secret"],
        token_url=context.resource_config["token_url"],
        data_endpoint=context.resource_config["data_endpoint"],
        query=context.resource_config["query"]
    )
