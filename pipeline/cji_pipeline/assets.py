import os
import duckdb
from dagster_duckdb import DuckDBResource
from dagster import asset
import pyarrow as pa
import pyarrow.parquet as pq

@asset(required_resource_keys={"linked_data_api"},)
def all_infras_file(context):
    """
    Fetches all infrastructures RDF data from UiTwisselingsplatform
    and saves the results as a Parquet file.
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
    Copies data from the Parquet file into DuckDB.
    """
    sql_query = """
        create or replace table all_infras as (
          select *
          from '../data/all_infras.parquet'
        );
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)
