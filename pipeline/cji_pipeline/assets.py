import os
import duckdb
from dagster_duckdb import DuckDBResource
from dagster import asset
import pyarrow as pa
import pyarrow.parquet as pq

@asset(required_resource_keys={"linked_data_api"},)
def all_infras_file(context):
    """
    Raw infrastructure data as fetched from UiTwisselingsplatform.
    """
    linked_data_api = context.resources.linked_data_api
    df = linked_data_api.fetch_data()

    # Save DataFrame as Parquet
    table = pa.Table.from_pandas(df)
    os.makedirs("../data", exist_ok=True)
    pq.write_table(table, "../data/all_infras.parquet")

    context.log.info(f"Saved {len(df)} results as Parquet: all_infras.parquet")

@asset(deps=["all_infras_file"])
def all_infras_table(database: DuckDBResource):
    """
    Cleaned infrastructure data in DuckDB with an ID and human readable labels.
    """
    sql_query = """
        CREATE OR REPLACE TEMPORARY TABLE temp_data AS 
        SELECT 
            locationName AS location_name,
            locationType AS location_type_uri,
            infraType AS infra_type_uri,
            thoroughfare AS street,
            huisnummer AS house_number,
            postCode AS postal_code,
            city, 
            bron as uwp_source_dp,
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
            gml,
        FROM '../data/all_infras.parquet';

        CREATE OR REPLACE TABLE all_infras AS
        SELECT row_number() OVER () AS id, * FROM temp_data;
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)
