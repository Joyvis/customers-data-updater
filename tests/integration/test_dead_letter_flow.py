"""
Integration tests: IT7 — Dead letter queue flow.

Acceptance criteria covered:
  AC-IT7.1: When AI returns "unclear" and message limit is exceeded, conversation becomes FAILED
             and BatchRecord becomes DEAD_LETTER
  AC-IT7.2: When AI returns "confirmed", conversation becomes COMPLETED and
             BatchRecord becomes COMPLETED
  AC-IT7.3: After all records reach a terminal state (mix of COMPLETED and DEAD_LETTER),
             batch status becomes PARTIALLY_COMPLETED
  AC-IT7.4: GET /batches/{id}/dead-letter returns records with status DEAD_LETTER
"""

import uuid
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import AsyncSession
from httpx import AsyncClient

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    Conversation,
    ConversationStatus,
    Tenant,
)
from app.services.outreach import process_inbound_message


async def _create_approved_batch_with_conversations(
    db_session: AsyncSession,
    tenant: Tenant,
    num_records: int = 3,
) -> tuple[Batch, list[BatchRecord], list[Conversation]]:
    """Create an APPROVED Batch with num_records OUTREACH BatchRecords and IN_PROGRESS Conversations."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="data.csv",
        file_key=f"{tenant.id}/{uuid.uuid4()}.csv",
        file_size=1024,
        status=BatchStatus.APPROVED,
        total_records=num_records,
        processed_records=num_records,
        max_messages_per_conversation=5,
    )
    db_session.add(batch)
    await db_session.flush()

    records = []
    conversations = []

    for i in range(num_records):
        record = BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=i + 1,
            phone_number=f"5511999990{i + 1:03d}",
            owner_name=f"Owner {i + 1}",
            entity_type="property",
            original_data={
                "phone_number": f"5511999990{i + 1:03d}",
                "owner_name": f"Owner {i + 1}",
            },
            status=BatchRecordStatus.OUTREACH,
        )
        db_session.add(record)
        await db_session.flush()

        conv = Conversation(
            batch_record_id=record.id,
            tenant_id=tenant.id,
            phone_number=record.phone_number,
            status=ConversationStatus.IN_PROGRESS,
            message_count=4,  # one more message will exceed max_messages=5
            max_messages=5,
        )
        db_session.add(conv)
        await db_session.flush()

        records.append(record)
        conversations.append(conv)

    await db_session.commit()
    for obj in [batch, *records, *conversations]:
        await db_session.refresh(obj)

    return batch, records, conversations


# ---------------------------------------------------------------------------
# IT7.1 — AI "unclear" + message limit exceeded → FAILED conversation + DEAD_LETTER record
# ---------------------------------------------------------------------------


async def test_it7_unclear_at_message_limit_produces_dead_letter(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT7.1: When AI returns 'unclear' and message count >= max_messages, conversation is FAILED
    and the BatchRecord becomes DEAD_LETTER."""
    batch, records, conversations = await _create_approved_batch_with_conversations(
        db_session, tenant, num_records=1
    )
    conv = conversations[0]
    record = records[0]

    unclear_result = {
        "classification": "unclear",
        "updated_fields": None,
        "follow_up_message": "Por favor, pode repetir?",
        "ai_reasoning": {},
        "classification_score": 0.3,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=unclear_result,
        ),
        patch("app.services.outreach.whatsapp.send_message", new_callable=AsyncMock),
        patch(
            "app.services.outreach.ai_conversation.build_system_prompt", return_value=""
        ),
        patch(
            "app.services.outreach.ai_conversation.build_user_context",
            return_value="context",
        ),
    ):
        await process_inbound_message(
            db_session,
            conv.id,
            "Não entendi",
            {"raw": True},
        )

    await db_session.refresh(conv)
    await db_session.refresh(record)

    assert conv.status == ConversationStatus.FAILED
    assert record.status == BatchRecordStatus.DEAD_LETTER


# ---------------------------------------------------------------------------
# IT7.2 — AI "confirmed" → COMPLETED conversation + COMPLETED record
# ---------------------------------------------------------------------------


async def test_it7_confirmed_produces_completed_status(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT7.2: When AI returns 'confirmed', conversation is COMPLETED and record is COMPLETED."""
    batch, records, conversations = await _create_approved_batch_with_conversations(
        db_session, tenant, num_records=1
    )
    conv = conversations[0]
    record = records[0]

    confirmed_result = {
        "classification": "confirmed",
        "updated_fields": None,
        "follow_up_message": "Obrigado!",
        "ai_reasoning": {},
        "classification_score": 0.95,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=confirmed_result,
        ),
        patch("app.services.outreach.whatsapp.send_message", new_callable=AsyncMock),
        patch(
            "app.services.outreach.ai_conversation.build_system_prompt", return_value=""
        ),
        patch(
            "app.services.outreach.ai_conversation.build_user_context",
            return_value="context",
        ),
    ):
        await process_inbound_message(
            db_session,
            conv.id,
            "Tudo certo!",
            {"raw": True},
        )

    await db_session.refresh(conv)
    await db_session.refresh(record)

    assert conv.status == ConversationStatus.COMPLETED
    assert record.status == BatchRecordStatus.COMPLETED


# ---------------------------------------------------------------------------
# IT7.3 — Mixed outcomes → batch becomes PARTIALLY_COMPLETED
# ---------------------------------------------------------------------------


async def test_it7_mixed_terminal_states_produce_partially_completed_batch(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT7.3: When some records are COMPLETED and some are DEAD_LETTER, batch is PARTIALLY_COMPLETED."""
    batch, records, conversations = await _create_approved_batch_with_conversations(
        db_session, tenant, num_records=3
    )

    unclear_result = {
        "classification": "unclear",
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {},
        "classification_score": 0.2,
    }
    confirmed_result = {
        "classification": "confirmed",
        "updated_fields": None,
        "follow_up_message": "Obrigado!",
        "ai_reasoning": {},
        "classification_score": 0.95,
    }

    # conversations[0] and [1] → unclear (dead letter); conversations[2] → confirmed (completed)
    with (
        patch("app.services.outreach.whatsapp.send_message", new_callable=AsyncMock),
        patch(
            "app.services.outreach.ai_conversation.build_system_prompt", return_value=""
        ),
        patch(
            "app.services.outreach.ai_conversation.build_user_context",
            return_value="context",
        ),
    ):
        for conv in conversations[:2]:
            with patch(
                "app.services.outreach.ai_conversation.process_response",
                return_value=unclear_result,
            ):
                await process_inbound_message(
                    db_session, conv.id, "Mensagem ambígua", {"raw": True}
                )

        with patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=confirmed_result,
        ):
            await process_inbound_message(
                db_session, conversations[2].id, "Confirmado!", {"raw": True}
            )

    await db_session.refresh(batch)
    assert batch.status == BatchStatus.PARTIALLY_COMPLETED


# ---------------------------------------------------------------------------
# IT7.4 — GET /batches/{id}/dead-letter returns DEAD_LETTER records
# ---------------------------------------------------------------------------


async def test_it7_dead_letter_endpoint_returns_failed_records(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT7.4: GET /batches/{id}/dead-letter returns records with DEAD_LETTER status."""
    batch, records, conversations = await _create_approved_batch_with_conversations(
        db_session, tenant, num_records=2
    )

    unclear_result = {
        "classification": "unclear",
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {},
        "classification_score": 0.1,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=unclear_result,
        ),
        patch("app.services.outreach.whatsapp.send_message", new_callable=AsyncMock),
        patch(
            "app.services.outreach.ai_conversation.build_system_prompt", return_value=""
        ),
        patch(
            "app.services.outreach.ai_conversation.build_user_context",
            return_value="context",
        ),
    ):
        for conv in conversations:
            await process_inbound_message(db_session, conv.id, "não sei", {"raw": True})

    response = await client.get(f"/batches/{batch.id}/dead-letter")
    assert response.status_code == 200, response.text

    dead_letter_items = response.json()
    assert len(dead_letter_items) == 2
    for item in dead_letter_items:
        assert item["status"] == BatchRecordStatus.DEAD_LETTER
