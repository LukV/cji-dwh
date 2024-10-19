from typing import Optional
from pydantic import BaseModel

class InfraBase(BaseModel):
    """
    Base model representing infrastructure details.
    """
    id: int
    location_name: Optional[str] = "De Bosuil"
    location_type_uri: Optional[str] = "http://infrastructuur.dcjm.be/id/type#jeugdverblijfOfJeugdhostel"
    location_type_label: Optional[str] = "Jeugdverblijf of Jeugdhostel"
    infra_type_uri: Optional[str] = "https://data.vlaanderen.be/ns/gebouw#Gebouw"
    street: Optional[str] = "Bosuilstraat"
    house_number: Optional[str] = "4"
    postal_code: Optional[str] = "3910"
    city: Optional[str] = "Sint-Huibrechts-Lille"
    uwp_source_dp: Optional[str] = None
    created_by: Optional[str] = None
    source_uri: str
    adresregister_uri: Optional[str] = None
    source_system: Optional[str] = None
    identifier: str
    localid: Optional[str] = None
    namespace: Optional[str] = None
    point: Optional[str] = None
    gml: Optional[str] = None

class InfraDetail(InfraBase):
    """
    Model representing detailed information about an infrastructure.
    Inherits from InfraBase.
    """
    pass

class InfraList(BaseModel):
    """
    Model representing a list of infrastructures with pagination information.
    """
    items: list[InfraBase]
    total: int = 1
    limit: int = 10
    offset: int = 0
