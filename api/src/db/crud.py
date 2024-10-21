from typing import List, Optional, Tuple
from .database import get_db_connection

def get_infras(limit: int = 10, offset: int = 0, filters: Optional[str] = None,
                sort_by: str = "id", sort_order: str = "asc") -> List[Tuple]:
    """
    Retrieve a list of infrastructures with pagination, filtering, and sorting.

    Args:
        limit (int): The maximum number of records to return. Default is 10.
        offset (int): The number of records to skip before starting to return records. Default is 0.
        filters (str): Boolean logic filter to filter the records by. Default is None.
        sort_by (str): The column to sort by. Default is "id".
        sort_order (str): The order in which to sort (either "asc" or "desc"). Default is "asc".

    Returns:
        List[Tuple]: A list of tuples representing the infrastructure records.
    """
    conn = get_db_connection()
    query = "SELECT * FROM all_infras"

    # Add filtering to the query if a filter term is provided
    if filters:
        query += f" WHERE {filters}"

    query += f" ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?"
    return conn.execute(query, (limit, offset)).fetchall()

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
