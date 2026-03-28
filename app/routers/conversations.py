import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.dependencies import get_current_user, get_db
from app.models.batch import BatchRecord
from app.models.conversation import (
    Conversation,
    ConversationStatus,
    Message,
    MessageDirection,
)
from app.models.tenant import TenantUser

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class MessageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    conversation_id: uuid.UUID
    direction: MessageDirection
    content: str
    ai_reasoning: dict | None
    classification_score: float | None
    raw_payload: dict | None
    created_at: str

    @classmethod
    def from_model(cls, msg: Message) -> "MessageResponse":
        return cls(
            id=msg.id,
            conversation_id=msg.conversation_id,
            direction=msg.direction,
            content=msg.content,
            ai_reasoning=msg.ai_reasoning,
            classification_score=msg.classification_score,
            raw_payload=msg.raw_payload,
            created_at=msg.created_at.isoformat(),
        )


class ConversationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_record_id: uuid.UUID
    tenant_id: uuid.UUID
    phone_number: str
    status: ConversationStatus
    classification: str | None
    message_count: int
    max_messages: int
    created_at: str
    updated_at: str
    completed_at: str | None

    @classmethod
    def from_model(cls, conv: Conversation) -> "ConversationResponse":
        return cls(
            id=conv.id,
            batch_record_id=conv.batch_record_id,
            tenant_id=conv.tenant_id,
            phone_number=conv.phone_number,
            status=conv.status,
            classification=conv.classification,
            message_count=conv.message_count,
            max_messages=conv.max_messages,
            created_at=conv.created_at.isoformat(),
            updated_at=conv.updated_at.isoformat(),
            completed_at=conv.completed_at.isoformat() if conv.completed_at else None,
        )


class ConversationDetailResponse(ConversationResponse):
    messages: list[MessageResponse]

    @classmethod
    def from_model_with_messages(
        cls, conv: Conversation
    ) -> "ConversationDetailResponse":
        base = ConversationResponse.from_model(conv)
        return cls(
            **base.model_dump(),
            messages=[MessageResponse.from_model(m) for m in conv.messages],
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/", response_model=list[ConversationResponse])
async def list_conversations(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    batch_id: Annotated[
        uuid.UUID | None, Query(description="Filter by batch ID")
    ] = None,
) -> list[ConversationResponse]:
    """List conversations for the current tenant, optionally filtered by batch."""
    query = select(Conversation).where(Conversation.tenant_id == current_user.tenant_id)

    if batch_id is not None:
        # Join through batch_records to filter by batch
        query = query.join(
            BatchRecord, Conversation.batch_record_id == BatchRecord.id
        ).where(BatchRecord.batch_id == batch_id)

    query = query.order_by(Conversation.created_at.desc())
    result = await db.execute(query)
    conversations = result.scalars().all()
    return [ConversationResponse.from_model(c) for c in conversations]


@router.get("/{conversation_id}", response_model=ConversationDetailResponse)
async def get_conversation(
    conversation_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> ConversationDetailResponse:
    """Get conversation detail with all messages, ai_reasoning, classification_scores."""
    result = await db.execute(
        select(Conversation)
        .options(selectinload(Conversation.messages))
        .where(
            Conversation.id == conversation_id,
            Conversation.tenant_id == current_user.tenant_id,
        )
    )
    conversation = result.scalar_one_or_none()
    if conversation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found",
        )
    return ConversationDetailResponse.from_model_with_messages(conversation)


@router.post("/webhooks/whatsapp", status_code=status.HTTP_200_OK)
async def whatsapp_webhook(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    """Receive and process incoming WhatsApp webhook events.

    Handles both the GET verification challenge from Meta and POST message events.
    """
    from app.services.whatsapp import parse_webhook_payload
    from app.tasks.outreach import process_inbound_message_task

    body = await request.json()

    # Verify webhook signature (mandatory)
    raw_body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not signature:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Missing webhook signature",
        )

    from app.services.whatsapp import verify_webhook_signature

    if not verify_webhook_signature(raw_body, signature):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid webhook signature",
        )

    parsed = parse_webhook_payload(body)
    if parsed is None:
        # Not a message event (e.g., status update) — acknowledge with 200
        return {"status": "ok", "processed": False}

    phone_number = parsed["phone_number"]
    message_content = parsed["content"]

    # Find the active (IN_PROGRESS) conversation for this phone number
    result = await db.execute(
        select(Conversation)
        .where(
            Conversation.phone_number == phone_number,
            Conversation.status == ConversationStatus.IN_PROGRESS,
        )
        .order_by(Conversation.updated_at.desc())
        .limit(1)
    )
    conversation = result.scalar_one_or_none()

    if conversation is None:
        logger.info(
            "No active IN_PROGRESS conversation found for phone %s — ignoring message",
            phone_number,
        )
        return {"status": "ok", "processed": False}

    # Queue the message for async processing
    process_inbound_message_task.delay(
        str(conversation.id),
        message_content,
        body,
    )

    return {"status": "ok", "processed": True, "conversation_id": str(conversation.id)}


@router.get("/webhooks/whatsapp", status_code=status.HTTP_200_OK)
async def whatsapp_webhook_verify(request: Request) -> int:
    """Handle Meta's webhook verification challenge (GET request)."""
    from app.config import settings

    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and token == settings.whatsapp_verify_token:
        if challenge is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing hub.challenge parameter",
            )
        return int(challenge)

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Webhook verification failed",
    )
