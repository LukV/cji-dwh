import os
from dagster import asset
import pyarrow as pa
import pyarrow.parquet as pq

@asset(required_resource_keys={"linked_data_api"},)
def all_infras_file(context):
    """
    This asset fetches data from a Linked Data API using a SPARQL query, processes the results, 
    and stores the output as a Parquet file.
    """
    linked_data_api = context.resources.linked_data_api

    # Fetch the data and transform it into a DataFrame
    df = linked_data_api.fetch_data()

    # Save DataFrame as Parquet
    table = pa.Table.from_pandas(df)
    os.makedirs("../data", exist_ok=True)
    pq.write_table(table, "../data/all_infras.parquet")

    context.log.info(f"Saved {len(df)} results as Parquet: all_infras.parquet")
