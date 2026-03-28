import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.models.batch import BatchRecordStatus, BatchStatus


class BatchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    file_name: str
    file_size: int
    status: BatchStatus
    total_records: int
    processed_records: int
    max_messages_per_conversation: int
    created_at: datetime
    updated_at: datetime


class BatchRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_id: uuid.UUID
    tenant_id: uuid.UUID
    row_number: int
    phone_number: str
    owner_name: str
    entity_type: str
    original_data: dict
    updated_data: dict | None
    status: BatchRecordStatus
    dedup_group_id: str | None
    created_at: datetime
    updated_at: datetime


class ValidationErrorResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_id: uuid.UUID
    row_number: int
    error_type: str
    message: str
    created_at: datetime
