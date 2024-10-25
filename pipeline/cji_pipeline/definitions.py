import os
from dagster import Definitions, load_assets_from_modules, EnvVar
from . import assets
from .resources import linked_data_api_resource, duckdb_resource, s3_resource

script_dir = os.path.dirname(os.path.abspath(__file__))
query_file_path = os.path.join(script_dir, "../queries/all_infras.sparql")
with open(query_file_path, encoding="utf-8") as qf:
    all_infras_query = qf.read()

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "linked_data_api": linked_data_api_resource.configured({
            "client_id": EnvVar("CLIENT_ID").get_value(),
            "client_secret": EnvVar("CLIENT_SECRET").get_value(),
            "token_url": EnvVar("TOKEN_URL").get_value(),
            "data_endpoint": EnvVar("DATA_ENDPOINT").get_value(),
            "query": all_infras_query
        }),
        "s3": s3_resource.configured({
            "aws_access_key_id": EnvVar("AWS_ACCESS_KEY_ID").get_value(),
            "aws_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
            "region_name": EnvVar("AWS_REGION").get_value(),  # Optional
            "s3_bucket_name": EnvVar("S3_BUCKET_NAME").get_value(),
        })
    }
)
