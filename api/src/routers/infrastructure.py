from typing import Optional
from fastapi import APIRouter, HTTPException, Query, status
from ..db import crud
from ..schemas.infrastructure import InfraList, InfraDetail, InfraBase

router = APIRouter()

ALL_COLS = ["id", "location_name", "location_type_uri", "location_type_label", "infra_type_uri",
            "street", "house_number", "postal_code", "city", "uwp_sourcd_dp", "created_by",
            "source_uri", "adresregister_uri", "source_system", "identifier", "localid", 
            "namespace", "point", "gml"]

@router.get("/infras", response_model=InfraList)
def read_infras(
        limit: int = Query(10, ge=1, description="The number of records to retrieve"),
        offset: int = Query(0, ge=0, description="The number of records to skip \
                                    before starting to return records"),
        filters: Optional[str] = Query(None, description="Boolean logic filter to filter \
                                    results by (e.g., 'city=Sint-Huibrechts-Lille AND \
                                    infra_type_uri=https://data.vlaanderen.be/ns/gebouw#Gebouw')"),
        sort_by: Optional[str] = Query("id", description="The column to sort by"),
        sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Sort \
                                    order: 'asc' or 'desc'")):
    """
    Retrieve a paginated list of infrastructure records with filtering and sorting.

    Args:
        limit (int): The maximum number of records to return.
        offset (int): The number of records to skip before starting to return records.
        filters (str): Boolean logic filter to filter the records by.
        sort_by (str): The column to sort by.
        sort_order (str): Sort order, either ascending ('asc') or descending ('desc').

    Returns:
        InfraList: A list of infrastructure records with pagination details.
    """
    # Ensure sort_by is a valid column
    if sort_by not in ALL_COLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sort_by parameter. Must be one of: {', '.join(ALL_COLS)}"
        )

    rows = crud.get_infras(
        limit=limit,
        offset=offset,
        filters=filters,
        sort_by=sort_by,
        sort_order=sort_order
    )

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No infrastructure records found."
        )

    # Convert each tuple row into an InfraBase instance using dictionary unpacking
    items = [InfraBase(**dict(zip(ALL_COLS, row)))
                for row in rows]

    total = len(items)
    return InfraList(items=items, total=total, limit=limit, offset=offset)



@router.get("/infras/{identifier}", response_model=InfraDetail)
def read_infra(identifier: str):
    """
    Retrieve the details of a specific infrastructure record by its identifier.

    Args:
        identifier (str): The unique identifier of the infrastructure record.

    Returns:
        InfraDetail: Detailed information about a single infrastructure record.
    """
    try:
        row = crud.get_infra_detail(identifier)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Infrastructure with id '{identifier}' not found."
            )

        # Convert tuple to InfraDetail using dictionary unpacking
        infra = InfraDetail(**dict(zip(ALL_COLS, row)))
        return infra

    except HTTPException as http_exc:
        # Re-raise HTTPExceptions to avoid double-handling
        raise http_exc

    except Exception as e:
        # Log the exception if necessary and return a 500 status code with a generic error message
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while retrieving \
                    the infrastructure record with id '{identifier}'."
        ) from e
