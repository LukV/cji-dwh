from typing import List, Tuple
from .database import get_db_connection

def get_infras(limit: int = 10, offset: int = 0) -> List[Tuple]:
    """
    Retrieve a list of infrastructures with pagination.

    Args:
        limit (int): The maximum number of records to return. Default is 10.
        offset (int): The number of records to skip before starting to return records. Default is 0.

    Returns:
        List[Tuple]: A list of tuples representing the infrastructure records.
    """
    conn = get_db_connection()
    query = f"SELECT * FROM all_infras LIMIT {limit} OFFSET {offset}"
    return conn.execute(query).fetchall()

def get_infra_detail(identifier: str) -> Tuple:
    """
    Retrieve details of a specific infrastructure by its identifier.

    Args:
        identifier (str): The unique identifier of the infrastructure.

    Returns:
        Tuple: A tuple representing the infrastructure record or None if not found.
    """
    conn = get_db_connection()
    query = "SELECT * FROM all_infras WHERE id = ?"
    return conn.execute(query, (identifier,)).fetchone()
