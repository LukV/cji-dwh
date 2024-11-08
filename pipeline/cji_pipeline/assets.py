import io
import re
from titlecase import titlecase
from dagster import MetadataValue, OpExecutionContext, asset, AssetIn
from duckdb.typing import VARCHAR
import pyarrow as pa
import pyarrow.parquet as pq

@asset(group_name="CJI", required_resource_keys={"linked_data_api"})
def raw_infras_data(context: OpExecutionContext):
    """
    Fetches raw infrastructure data from UiTwisselingsplatform.
    """
    linked_data_api = context.resources.linked_data_api
    df = linked_data_api.fetch_data()

    # Convert DataFrame to Parquet bytes
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    # Return the buffer
    return parquet_buffer

@asset(
    group_name="CJI",
    required_resource_keys={"s3"},
    ins={"raw_infras_data": AssetIn()},
)
def raw_infras_data_s3(context: OpExecutionContext, raw_infras_data): # pylint: disable=W0621
    """
    Uploads the raw infrastructure Parquet data to S3.
    """
    s3_client = context.resources.s3
    bucket_name = s3_client.bucket_name
    s3_key = "raw_infras_data.parquet"

    # Upload the buffer to S3
    try:
        s3_client.upload_fileobj(
            Fileobj=raw_infras_data,
            Bucket=bucket_name,
            Key=s3_key,
        )
        context.log.info(f"Uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        context.log.error(f"Failed to upload to S3: {e}")
        raise

    # Return the S3 key for downstream assets
    return s3_key

@asset(
    group_name="CJI",
    required_resource_keys={"s3", "duckdb"},
    ins={"raw_infras_data": AssetIn()},
)
def all_infras(context: OpExecutionContext, raw_infras_data): # pylint: disable=W0621
    """
    Processes the infrastructure data using DuckDB, reading from S3.
    """
    # Read Parquet data from the buffer into DuckDB
    duckdb = context.resources.duckdb
    s3_client = context.resources.s3
    bucket_name = s3_client.bucket_name

    # Create an in-memory DuckDB connection
    with duckdb.get_connection() as conn:
        # Read the Parquet data from the buffer
        # Note: DuckDB can read from file-like objects using `read_parquet`
        df = pq.read_table(raw_infras_data).to_pandas()
        conn.register("temp_raw_data", df)

        # Define the split_camel_case function in DuckDB
        def split_camel_case(value):
            if value and "#" in value:
                value = value.split("#")[-1]
                value = re.sub(r'(?<!^)([A-Z])', r' \1', value)
                value = titlecase(value.lower())
            return value

        conn.create_function('split_camel_case', split_camel_case, [(VARCHAR)], VARCHAR)

        # Perform SQL operations
        sql_query = """
            CREATE OR REPLACE TABLE all_infras AS
            SELECT row_number() OVER () AS id,
                   locationName AS location_name,
                   locationType AS location_type_uri,
                   split_camel_case(locationType) AS location_type_label,
                   infraType AS infra_type_uri,
                   thoroughfare AS street,
                   huisnummer AS house_number,
                   postCode AS postal_code,
                   city,
                   bron AS uwp_source_dp,
                   createdBy AS created_by,
                   subject AS source_uri,
                   adresregisteruri AS adresregister_uri,
                   CASE
                       WHEN namespace = 'https://kampas.be/id/gebouw/' THEN 'Kampas'
                       WHEN namespace = 'https://erfgoedkaart.be/id/infrastructuur/' THEN 'Erfgoedkaart'
                       WHEN namespace = 'https://data.publiq.be/id/place/udb/' THEN 'UiTdatabank'
                       WHEN namespace = 'https://terra.be/id/infrastructuur/' THEN 'Terra'
                       WHEN namespace = 'https://www.jeugdmaps.be/id/buitenruimte/' THEN 'Jeugdmaps'
                       WHEN namespace = 'https://natuurenbos.vlaanderen.be/id/buitenruimte/' THEN 'Natuur en bos'
                       WHEN namespace = 'https://www.jeugdmaps.be/id/gebouw/' THEN 'Jeugdmaps'
                       ELSE NULL
                   END AS source_system,
                   identifier,
                   localid,
                   namespace,
                   point,
                   gml
            FROM temp_raw_data;
        """

        conn.execute(sql_query)

        # Fetch the processed data into a DataFrame
        processed_df = conn.execute("SELECT * FROM all_infras").fetchdf()

    # Convert the processed DataFrame to Parquet bytes
    processed_table = pa.Table.from_pandas(processed_df)
    processed_parquet_buffer = io.BytesIO()
    pq.write_table(processed_table, processed_parquet_buffer)
    processed_parquet_buffer.seek(0)

    # Upload the processed Parquet data to S3
    s3_key = "all_infras_final.parquet"
    try:
        s3_client.upload_fileobj(
            Fileobj=processed_parquet_buffer,
            Bucket=bucket_name,
            Key=s3_key,
        )
        context.log.info(f"Processed data uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        context.log.error(f"Failed to upload processed data to S3: {e}")
        raise

    # Optionally, add output metadata
    context.add_output_metadata(
        {
            "num_records": len(processed_df),
            "s3_path": f"s3://{bucket_name}/{s3_key}",
            "preview": MetadataValue.md(processed_df.head().to_markdown()),
        }
    )

    # Return the S3 key for downstream assets if needed
    return s3_key
