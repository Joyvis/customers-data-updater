import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class MessageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    conversation_id: uuid.UUID
    direction: str
    content: str
    ai_reasoning: dict | None
    classification_score: float | None
    raw_payload: dict | None
    created_at: datetime


class ConversationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_record_id: uuid.UUID
    phone_number: str
    status: str
    classification: str | None
    message_count: int
    created_at: datetime
    messages: list[MessageResponse] = []
