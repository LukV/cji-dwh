import os
import duckdb

def get_db_connection():
    """
    Establish a connection to the DuckDB database.

    Returns:
        duckdb.DuckDBPyConnection: A connection object to interact with the database.
    """
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../data/cji.db'))
    return duckdb.connect(db_path)
