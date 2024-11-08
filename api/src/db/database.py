from .cache_manager import CacheManager

# Create a single instance of CacheManager for the application
cache_manager = CacheManager()

def get_db_connection():
    """
    Get a DuckDB connection with the cached data registered.
    """
    return cache_manager.create_duckdb_connection()
