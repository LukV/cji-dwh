from pydantic import BaseModel
from typing import Optional

class InfraBase(BaseModel):
    subject: Optional[str] = None
    locationName: Optional[str] = None
    locationType: Optional[str] = None
    gml: Optional[str] = None
    bron: Optional[str] = None
    infraType: Optional[str] = None
    createdBy: Optional[str] = None
    identifier: str = "unique-identifier-12345"
    localid: Optional[str] = None
    namespace: Optional[str] = None
    thoroughfare: Optional[str] = None
    huisnummer: Optional[str] = None
    fullAddress: Optional[str] = None
    postCode: Optional[str] = "1000"
    city: Optional[str] = "Brussels"
    point: Optional[str] = None
    adresregisteruri: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "subject": "Infrastructure Subject Example",
                "locationName": "Brussels Central Station",
                "locationType": "Train Station",
                "gml": "<gml>Example</gml>",
                "bron": "Flemish Government",
                "infraType": "Transport Hub",
                "createdBy": "user@example.com",
                "identifier": "unique-identifier-12345",
                "localid": "local-001",
                "namespace": "http://example.com/namespace",
                "thoroughfare": "Rue des Fleurs",
                "huisnummer": "23A",
                "fullAddress": "23A Rue des Fleurs, 1000 Brussels",
                "postCode": "1000",
                "city": "Brussels",
                "point": "POINT(4.3517 50.8503)",
                "adresregisteruri": "http://example.com/address/12345"
            }
        }

class InfraDetail(InfraBase):
    pass

class InfraList(BaseModel):
    items: list[InfraBase]
    total: int = 1
    limit: int = 10
    offset: int = 0

    class Config:
        schema_extra = {
            "example": {
                "items": [
                    {
                        "subject": "Infrastructure Subject Example",
                        "locationName": "Brussels Central Station",
                        "locationType": "Train Station",
                        "gml": "<gml>Example</gml>",
                        "bron": "Flemish Government",
                        "infraType": "Transport Hub",
                        "createdBy": "user@example.com",
                        "identifier": "unique-identifier-12345",
                        "localid": "local-001",
                        "namespace": "http://example.com/namespace",
                        "thoroughfare": "Rue des Fleurs",
                        "huisnummer": "23A",
                        "fullAddress": "23A Rue des Fleurs, 1000 Brussels",
                        "postCode": "1000",
                        "city": "Brussels",
                        "point": "POINT(4.3517 50.8503)",
                        "adresregisteruri": "http://example.com/address/12345"
                    }
                ],
                "total": 1,
                "limit": 10,
                "offset": 0
            }
        }
