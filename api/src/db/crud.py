# src/db/crud.py

from typing import List, Optional, Tuple
from .database import get_db_connection

def get_infras(limit: int = 10, offset: int = 0, filters: Optional[dict] = None,
               sort_by: str = "id", sort_order: str = "asc") -> List[Tuple]:
    """
    Retrieve a list of infrastructures with pagination, filtering, and sorting.
    """
    conn = get_db_connection()

    query = "SELECT * FROM all_infras"
    params = []

    # Add filtering to the query if filters are provided
    if filters:
        filter_clauses = []
        for key, value in filters.items():
            filter_clauses.append(f"{key} = ?")
            params.append(value)
        query += " WHERE " + " AND ".join(filter_clauses)

    # Validate and sanitize sort_by and sort_order
    allowed_sort_columns = ["id", "location_name", "city", "source_system"]
    if sort_by not in allowed_sort_columns:
        sort_by = "id"

    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "asc"

    query += f" ORDER BY {sort_by} {sort_order.upper()} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    return conn.execute(query, params).fetchall()

def get_infra_detail(identifier: str) -> Tuple:
    """
    Retrieve details of a specific infrastructure by its identifier.
    """
    conn = get_db_connection()
    query = "SELECT * FROM all_infras WHERE id = ?"
    return conn.execute(query, (identifier,)).fetchone()
