import uuid

from pydantic import BaseModel, ConfigDict


class ColumnMappingCreate(BaseModel):
    entity_type: str
    original_name: str
    friendly_name: str


class ColumnMappingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    entity_type: str
    original_name: str
    friendly_name: str


class EntityTypeConfigResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    entity_type: str
    required_columns: list
