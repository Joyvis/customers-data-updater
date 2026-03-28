import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.batch import Batch, BatchRecord, BatchRecordStatus, BatchStatus
from app.models.conversation import (
    Conversation,
    ConversationStatus,
    Message,
    MessageDirection,
)
from app.models.mapping import ColumnMapping
from app.models.opt_out import OptOutList
from app.services import ai_conversation, usage, whatsapp

logger = logging.getLogger(__name__)


async def _load_column_mappings(
    db: AsyncSession, tenant_id: uuid.UUID, entity_type: str
) -> dict:
    """Return {original_name: friendly_name} for a tenant + entity_type pair."""
    result = await db.execute(
        select(ColumnMapping).where(
            ColumnMapping.tenant_id == tenant_id,
            ColumnMapping.entity_type == entity_type,
        )
    )
    mappings = result.scalars().all()
    return {m.original_name: m.friendly_name for m in mappings}


async def _build_conversation_history(messages: list[Message]) -> list[dict]:
    """Convert stored Message rows to Anthropic-style message dicts."""
    history = []
    for msg in messages:
        role = "assistant" if msg.direction == MessageDirection.OUTBOUND else "user"
        history.append({"role": role, "content": msg.content})
    return history


async def start_batch_outreach(db: AsyncSession, batch_id: uuid.UUID) -> None:
    """Load all READY conversations for a batch and send initial template messages.

    Args:
        db: Async SQLAlchemy session.
        batch_id: UUID of the batch to process.
    """
    batch_result = await db.execute(
        select(Batch).options(selectinload(Batch.tenant)).where(Batch.id == batch_id)
    )
    batch = batch_result.scalar_one_or_none()
    if batch is None:
        logger.error("Batch %s not found", batch_id)
        return

    # Load all READY conversations for this batch via batch_records
    records_result = await db.execute(
        select(BatchRecord)
        .options(selectinload(BatchRecord.conversations))
        .where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.status == BatchRecordStatus.READY,
        )
    )
    records = records_result.scalars().all()

    tenant_name = batch.tenant.name if batch.tenant else "nossa empresa"

    for record in records:
        # Find or pick the first READY conversation for this record
        ready_convs = [
            c for c in record.conversations if c.status == ConversationStatus.READY
        ]
        if not ready_convs:
            logger.warning("BatchRecord %s has no READY conversations", record.id)
            continue

        conversation = ready_convs[0]

        # Build template params with owner name and entity context
        template_params = {
            "template_name": "data_refresh_request",
            "language_code": "pt_BR",
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": record.owner_name},
                        {"type": "text", "text": tenant_name},
                    ],
                }
            ],
        }

        try:
            api_response = await whatsapp.send_template_message(
                phone_number=record.phone_number,
                template_params=template_params,
            )

            # Record outbound message
            outbound_msg = Message(
                conversation_id=conversation.id,
                tenant_id=record.tenant_id,
                direction=MessageDirection.OUTBOUND,
                content=f"[template: data_refresh_request] Olá {record.owner_name}, somos da {tenant_name}...",
                raw_payload=api_response,
            )
            db.add(outbound_msg)

            # Transition conversation to IN_PROGRESS
            conversation.status = ConversationStatus.IN_PROGRESS
            conversation.message_count += 1

            # Transition record to OUTREACH
            record.status = BatchRecordStatus.OUTREACH

            # Record usage events
            await usage.record_event(db, record.tenant_id, "conversation_started")
            await usage.record_event(db, record.tenant_id, "message_sent")

            logger.info(
                "Sent initial outreach for conversation %s (record %s)",
                conversation.id,
                record.id,
            )
        except Exception as exc:
            logger.error(
                "Failed to send template to %s (conversation %s): %s",
                record.phone_number,
                conversation.id,
                exc,
            )
            conversation.status = ConversationStatus.FAILED
            record.status = BatchRecordStatus.DEAD_LETTER

    await db.commit()


async def process_inbound_message(
    db: AsyncSession,
    conversation_id: uuid.UUID,
    message_content: str,
    raw_payload: dict,
) -> None:
    """Process an inbound WhatsApp message for a conversation.

    Steps:
    1. Load conversation + batch_record + tenant column mappings
    2. Build conversation history from existing Messages
    3. Call ai_conversation.process_response
    4. Store inbound Message with raw_payload
    5. Store outbound Message (AI response) with ai_reasoning and classification_score
    6. Handle classification outcome
    7. Check batch completion

    Args:
        db: Async SQLAlchemy session.
        conversation_id: UUID of the conversation to process.
        message_content: Text content of the inbound message.
        raw_payload: Raw webhook payload dict for audit trail.
    """
    conv_result = await db.execute(
        select(Conversation)
        .options(
            selectinload(Conversation.messages),
            selectinload(Conversation.batch_record)
            .selectinload(BatchRecord.batch)
            .selectinload(Batch.tenant),
        )
        .where(Conversation.id == conversation_id)
    )
    conversation = conv_result.scalar_one_or_none()
    if conversation is None:
        logger.error("Conversation %s not found", conversation_id)
        return

    batch_record = conversation.batch_record
    batch = batch_record.batch
    tenant = batch.tenant

    # If conversation is already terminal, ignore
    if conversation.status in (
        ConversationStatus.COMPLETED,
        ConversationStatus.FAILED,
        ConversationStatus.CANCELLED,
    ):
        logger.info(
            "Ignoring inbound message for already-terminal conversation %s (status=%s)",
            conversation_id,
            conversation.status,
        )
        return

    column_mappings = await _load_column_mappings(
        db, batch_record.tenant_id, batch_record.entity_type
    )

    system_prompt = ai_conversation.build_system_prompt(
        entity_type=batch_record.entity_type,
        column_mappings=column_mappings,
        tenant_name=tenant.name,
    )

    user_context = ai_conversation.build_user_context(
        original_data=batch_record.original_data,
        column_mappings=column_mappings,
    )

    # Build history: include user_context as a synthetic first assistant message if no history yet
    history = await _build_conversation_history(conversation.messages)
    if not history:
        history = [{"role": "assistant", "content": user_context}]

    # Call AI
    ai_result = ai_conversation.process_response(
        conversation_history=history,
        original_data=batch_record.original_data,
        owner_response=message_content,
        system_prompt=system_prompt,
    )

    classification = ai_result["classification"]
    updated_fields = ai_result.get("updated_fields")
    follow_up_message = ai_result.get("follow_up_message")
    ai_reasoning = ai_result.get("ai_reasoning", {})
    classification_score = ai_result.get("classification_score", 0.5)

    # Store inbound message
    inbound_msg = Message(
        conversation_id=conversation.id,
        tenant_id=batch_record.tenant_id,
        direction=MessageDirection.INBOUND,
        content=message_content,
        raw_payload=raw_payload,
    )
    db.add(inbound_msg)
    conversation.message_count += 1

    # Record usage for inbound message and AI API call
    await usage.record_event(db, batch_record.tenant_id, "message_received")
    await usage.record_event(db, batch_record.tenant_id, "api_call_made")

    # Handle classification
    if classification == "confirmed":
        batch_record.updated_data = batch_record.original_data
        _complete_conversation(conversation, classification)
        batch_record.status = BatchRecordStatus.COMPLETED

    elif classification == "updated":
        merged = dict(batch_record.original_data)
        if updated_fields:
            merged.update(updated_fields)
        batch_record.updated_data = merged
        _complete_conversation(conversation, classification)
        batch_record.status = BatchRecordStatus.COMPLETED

    elif classification == "unclear":
        max_msgs = conversation.max_messages
        if conversation.message_count < max_msgs and follow_up_message:
            # Send follow-up
            try:
                await whatsapp.send_message(
                    phone_number=conversation.phone_number,
                    text=follow_up_message,
                )
                outbound_msg = Message(
                    conversation_id=conversation.id,
                    tenant_id=batch_record.tenant_id,
                    direction=MessageDirection.OUTBOUND,
                    content=follow_up_message,
                    ai_reasoning=ai_reasoning,
                    classification_score=classification_score,
                )
                db.add(outbound_msg)
                conversation.message_count += 1
                await usage.record_event(db, batch_record.tenant_id, "message_sent")
                # Ensure conversation remains IN_PROGRESS
                conversation.status = ConversationStatus.IN_PROGRESS
            except Exception as exc:
                logger.error(
                    "Failed to send follow-up for conversation %s: %s",
                    conversation_id,
                    exc,
                )
        else:
            # Exceeded message limit — dead letter
            conversation.status = ConversationStatus.FAILED
            conversation.classification = "unclear"
            conversation.completed_at = datetime.now(timezone.utc)
            batch_record.status = BatchRecordStatus.DEAD_LETTER

    elif classification in ("refused", "opt_out"):
        conversation.status = ConversationStatus.CANCELLED
        conversation.classification = classification
        conversation.completed_at = datetime.now(timezone.utc)
        batch_record.status = BatchRecordStatus.DEAD_LETTER

        if classification == "opt_out":
            # Add to opt-out list
            opt_out = OptOutList(
                tenant_id=batch_record.tenant_id,
                phone_number=conversation.phone_number,
                reason="opt_out via WhatsApp conversation",
            )
            db.add(opt_out)

    else:
        # Fallback for unexpected classification values
        logger.warning(
            "Unexpected classification '%s' for conversation %s",
            classification,
            conversation_id,
        )
        conversation.status = ConversationStatus.FAILED
        batch_record.status = BatchRecordStatus.DEAD_LETTER

    # For terminal classifications, send and store a closing outbound message.
    # The AI may provide a follow_up_message as the farewell; fall back to a default.
    if classification in ("confirmed", "updated", "refused", "opt_out"):
        response_text = follow_up_message or _build_terminal_message(
            classification, tenant.name
        )
        try:
            await whatsapp.send_message(
                phone_number=conversation.phone_number,
                text=response_text,
            )
        except Exception as exc:
            logger.warning(
                "Failed to send terminal message for conversation %s: %s",
                conversation_id,
                exc,
            )
        outbound_msg = Message(
            conversation_id=conversation.id,
            tenant_id=batch_record.tenant_id,
            direction=MessageDirection.OUTBOUND,
            content=response_text,
            ai_reasoning=ai_reasoning,
            classification_score=classification_score,
        )
        db.add(outbound_msg)
        await usage.record_event(db, batch_record.tenant_id, "message_sent")

    await db.commit()

    # Check if the batch is now fully complete
    await check_batch_completion(db, batch.id)


def _complete_conversation(conversation: Conversation, classification: str) -> None:
    conversation.status = ConversationStatus.COMPLETED
    conversation.classification = classification
    conversation.completed_at = datetime.now(timezone.utc)


def _build_terminal_message(classification: str, tenant_name: str) -> str:
    if classification == "confirmed":
        return f"Obrigado pela confirmação! Seus dados estão atualizados em nosso sistema. Atenciosamente, {tenant_name}."
    elif classification == "updated":
        return f"Obrigado! Suas informações foram atualizadas com sucesso. Atenciosamente, {tenant_name}."
    elif classification == "refused":
        return f"Tudo bem! Não entraremos em contato novamente sobre este assunto. Atenciosamente, {tenant_name}."
    elif classification == "opt_out":
        return f"Entendido. Seu número foi removido de nossa lista de contatos. Atenciosamente, {tenant_name}."
    return "Encerramos nossa conversa. Obrigado."


async def check_batch_completion(db: AsyncSession, batch_id: uuid.UUID) -> None:
    """Check if all records in a batch are resolved and update batch status.

    Args:
        db: Async SQLAlchemy session.
        batch_id: UUID of the batch to check.
    """
    batch_result = await db.execute(select(Batch).where(Batch.id == batch_id))
    batch = batch_result.scalar_one_or_none()
    if batch is None:
        return

    records_result = await db.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch_id)
    )
    records = records_result.scalars().all()

    if not records:
        return

    terminal_statuses = {
        BatchRecordStatus.COMPLETED,
        BatchRecordStatus.DEAD_LETTER,
        BatchRecordStatus.SKIPPED,
        BatchRecordStatus.OPTED_OUT,
    }

    all_done = all(r.status in terminal_statuses for r in records)
    if not all_done:
        return

    completed_count = sum(1 for r in records if r.status == BatchRecordStatus.COMPLETED)
    total = len(records)

    if completed_count == total:
        batch.status = BatchStatus.COMPLETED
    elif completed_count > 0:
        batch.status = BatchStatus.PARTIALLY_COMPLETED
    else:
        batch.status = BatchStatus.PARTIALLY_COMPLETED

    await db.commit()
    logger.info(
        "Batch %s completed: %d/%d records completed (status=%s)",
        batch_id,
        completed_count,
        total,
        batch.status,
    )
