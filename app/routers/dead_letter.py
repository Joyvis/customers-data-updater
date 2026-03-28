import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.dependencies import get_current_user, get_db
from app.models.batch import Batch, BatchRecord, BatchRecordStatus
from app.models.conversation import Conversation, ConversationStatus
from app.models.tenant import TenantUser

router = APIRouter()


class DeadLetterRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_id: uuid.UUID
    row_number: int
    phone_number: str
    owner_name: str
    entity_type: str
    original_data: dict
    status: BatchRecordStatus
    reason: str
    created_at: str
    updated_at: str


@router.get("/{batch_id}/dead-letter", response_model=list[DeadLetterRecordResponse])
async def list_dead_letter_records(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> list[DeadLetterRecordResponse]:
    """List dead letter records for a batch.

    Returns BatchRecords with status DEAD_LETTER, including the reason derived
    from the associated conversation status (FAILED or CANCELLED).

    Args:
        batch_id: UUID of the batch to query.
    """
    # Verify the batch belongs to the current tenant
    batch_result = await db.execute(
        select(Batch).where(
            Batch.id == batch_id,
            Batch.tenant_id == current_user.tenant_id,
        )
    )
    batch = batch_result.scalar_one_or_none()
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found",
        )

    records_result = await db.execute(
        select(BatchRecord)
        .options(selectinload(BatchRecord.conversations))
        .where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
            BatchRecord.status == BatchRecordStatus.DEAD_LETTER,
        )
        .order_by(BatchRecord.row_number)
    )
    records = records_result.scalars().all()

    response_items = []
    for record in records:
        reason = _determine_reason(record.conversations)
        response_items.append(
            DeadLetterRecordResponse(
                id=record.id,
                batch_id=record.batch_id,
                row_number=record.row_number,
                phone_number=record.phone_number,
                owner_name=record.owner_name,
                entity_type=record.entity_type,
                original_data=record.original_data,
                status=record.status,
                reason=reason,
                created_at=record.created_at.isoformat(),
                updated_at=record.updated_at.isoformat(),
            )
        )

    return response_items


def _determine_reason(conversations: list[Conversation]) -> str:
    """Derive a human-readable reason from the conversation statuses.

    Priority: CANCELLED (refused/opt_out) > FAILED (message limit exceeded or error) > unknown.
    """
    if not conversations:
        return "unknown"

    statuses = {c.status for c in conversations}

    if ConversationStatus.CANCELLED in statuses:
        # Find the cancelled conversation to check classification
        for conv in conversations:
            if conv.status == ConversationStatus.CANCELLED:
                if conv.classification == "opt_out":
                    return "opt_out"
                return "refused"

    if ConversationStatus.FAILED in statuses:
        return "failed"

    return "dead_letter"
