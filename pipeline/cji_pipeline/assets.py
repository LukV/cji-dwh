import io
import re
import json
from titlecase import titlecase
from dagster import MetadataValue, OpExecutionContext, asset, AssetIn
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import Transformer
import pandas as pd

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
def raw_infras_data_s3(context: OpExecutionContext, raw_infras_data):  # pylint: disable=W0621
    """
    Processes the raw infrastructure data to include GeoJSON with WGS84 coordinates
    and uploads it to S3.
    """
    s3_client = context.resources.s3
    bucket_name = s3_client.bucket_name
    s3_key = "raw_infras_data.parquet"

    # Read Parquet data from the buffer into a DataFrame
    df = pq.read_table(raw_infras_data).to_pandas()

    # Define transformers
    lambert72_to_wgs84 = Transformer.from_crs("EPSG:31370", "EPSG:4326", always_xy=True)

    def create_geojson(row):
        point_field = row.get('point')
        gml_field = row.get('gml')

        # Helper function to parse srsName
        def get_srs_name(gml_string):
            match = re.search(r'srsName=["\']([^"\']+)["\']', gml_string)
            if match:
                return match.group(1)
            return None

        # Determine CRS based on srsName
        def get_transformer(srs_name):
            if srs_name:
                if '31370' in srs_name:
                    return lambert72_to_wgs84
                elif 'crs84' in srs_name.lower() or '4326' in srs_name:
                    return None  # Already in WGS84
            return None  # Default to WGS84 if unknown

        # Process point data
        if pd.notnull(point_field):
            srs_name = get_srs_name(point_field)
            transformer = get_transformer(srs_name)

            # Handle different point formats
            coord_match = re.search(r'<gml:coordinates>([^<]+)</gml:coordinates>', point_field)
            if not coord_match:
                coord_match = re.search(r'<gml:pos>([^<]+)</gml:pos>', point_field)
            if coord_match:
                coords_str = coord_match.group(1).strip()
                # Split on commas or whitespace
                coords = re.split(r'[,\s]+', coords_str)
                # Clean up coordinate strings
                coords = [coord.strip() for coord in coords if coord.strip()]
                if len(coords) >= 2:
                    try:
                        x_coord, y_coord = map(float, coords[:2])

                        if transformer:
                            lon, lat = transformer.transform(x_coord, y_coord)
                        else:
                            lon, lat = x_coord, y_coord

                        # Create GeoJSON Point feature
                        geojson_geometry = {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        }
                        return json.dumps(geojson_geometry)
                    except ValueError as e:
                        context.log.error(f"Error converting coordinates to float for row {row.name}: {e}")
                        return None
                else:
                    context.log.error(f"Insufficient coordinates in point for row {row.name}")
                    return None

        # Process polygon data
        elif pd.notnull(gml_field):
            srs_name = get_srs_name(gml_field)
            transformer = get_transformer(srs_name)

            pos_list_match = re.search(r'<gml:posList>([^<]+)</gml:posList>', gml_field)
            if pos_list_match:
                coord_list_str = pos_list_match.group(1).strip()
                coord_list = re.split(r'\s+', coord_list_str)
                coords = list(map(float, coord_list))
                if len(coords) % 2 != 0:
                    context.log.error(f"Invalid number of coordinates in posList for row {row.name}")
                    return None

                points = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]

                if transformer:
                    transformed_points = [transformer.transform(x, y) for x, y in points]
                else:
                    transformed_points = points

                # Create GeoJSON Polygon feature
                geojson_geometry = {
                    "type": "Polygon",
                    "coordinates": [transformed_points]
                }
                return json.dumps(geojson_geometry)
            else:
                context.log.error(f"No posList found in gml for row {row.name}")
                return None
        else:
            return None

    # Apply the function to create the 'geojson' field
    df['geojson'] = df.apply(create_geojson, axis=1)

    # Map 'namespace' to 'source_system'
    source_system_mapping = {
        'https://kampas.be/id/gebouw/': 'Kampas',
        'https://erfgoedkaart.be/id/infrastructuur/': 'Erfgoedkaart',
        'https://data.publiq.be/id/place/udb/': 'UiTdatabank',
        'https://terra.be/id/infrastructuur/': 'Terra',
        'https://www.jeugdmaps.be/id/buitenruimte/': 'Jeugdmaps',
        'https://natuurenbos.vlaanderen.be/id/buitenruimte/': 'Natuur en bos',
        'https://www.jeugdmaps.be/id/gebouw/': 'Jeugdmaps',
        # Add other mappings as needed
    }
    df['source_system'] = df['namespace'].map(source_system_mapping)

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

    # Upload the buffer to S3
    try:
        s3_client.upload_fileobj(
            Fileobj=parquet_buffer,
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
    ins={"raw_infras_data_s3": AssetIn()},
)
def all_infras(context: OpExecutionContext, raw_infras_data_s3):  # pylint: disable=W0621
    """
    Processes the infrastructure data using DuckDB, reading from S3.
    """
    # Read Parquet data from S3 into DuckDB
    duckdb = context.resources.duckdb
    s3_client = context.resources.s3
    bucket_name = s3_client.bucket_name
    s3_key = raw_infras_data_s3

    # Download the Parquet file from S3
    parquet_buffer = io.BytesIO()
    s3_client.download_fileobj(Bucket=bucket_name, Key=s3_key, Fileobj=parquet_buffer)
    parquet_buffer.seek(0)

    # Read Parquet data into a DataFrame
    df = pq.read_table(parquet_buffer).to_pandas()

    # Create an in-memory DuckDB connection
    with duckdb.get_connection() as conn:
        # Register the DataFrame as a DuckDB table
        conn.register("temp_raw_data", df)

        # Define the split_camel_case function in DuckDB
        def split_camel_case(value):
            if value and "#" in value:
                value = value.split("#")[-1]
                value = re.sub(r'(?<!^)([A-Z])', r' \1', value)
                value = titlecase(value.lower())
            return value

        conn.create_function('split_camel_case', split_camel_case, ['VARCHAR'], 'VARCHAR')

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
                   perceeluri AS perceel_uri,
                   source_system,
                   identifier,
                   localid,
                   namespace,
                   point,
                   gml,
                   geojson
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
    s3_key_final = "all_infras_final.parquet"
    try:
        s3_client.upload_fileobj(
            Fileobj=processed_parquet_buffer,
            Bucket=bucket_name,
            Key=s3_key_final,
        )
        context.log.info(f"Processed data uploaded to s3://{bucket_name}/{s3_key_final}")
    except Exception as e:
        context.log.error(f"Failed to upload processed data to S3: {e}")
        raise

    # Optionally, add output metadata
    context.add_output_metadata(
        {
            "num_records": len(processed_df),
            "s3_path": f"s3://{bucket_name}/{s3_key_final}",
            "preview": MetadataValue.md(processed_df.head().to_markdown()),
        }
    )

    # Return the S3 key for downstream assets if needed
    return s3_key_final

