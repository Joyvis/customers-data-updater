"""
Unit tests for audit and LGPD data-erasure functionality.

T24: AC-Audit-1 — Conversation detail endpoint returns all messages with reasoning fields
T25: AC-Audit-2 — Data erasure deletes all records for a phone number across batches
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    Conversation,
    ConversationStatus,
    Message,
    MessageDirection,
    Tenant,
    TenantUser,
)
from app.services.erasure import erase_data_for_phone


# ---------------------------------------------------------------------------
# T24: Conversation detail returns all messages with reasoning fields
# AC-Audit-1: GET /conversations/{id} must include ai_reasoning, classification_score,
#             and raw_payload on each message.
# ---------------------------------------------------------------------------


async def test_t24_conversation_detail_includes_all_message_fields(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T24 — AC-Audit-1: Conversation detail response contains full message audit fields."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="audit.csv",
        file_key="test/audit.csv",
        file_size=512,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990070",
        owner_name="Beatriz Lima",
        entity_type="property",
        original_data={"phone_number": "5511999990070", "owner_name": "Beatriz Lima"},
        status=BatchRecordStatus.OUTREACH,
    )
    db_session.add(record)
    await db_session.flush()

    conversation = Conversation(
        batch_record_id=record.id,
        tenant_id=tenant.id,
        phone_number="5511999990070",
        status=ConversationStatus.IN_PROGRESS,
        message_count=2,
        max_messages=5,
    )
    db_session.add(conversation)
    await db_session.flush()

    outbound_msg = Message(
        conversation_id=conversation.id,
        tenant_id=tenant.id,
        direction=MessageDirection.OUTBOUND,
        content="Olá Beatriz, seus dados estão corretos?",
        ai_reasoning=None,
        classification_score=None,
        raw_payload={"template": "data_refresh_request"},
    )
    inbound_msg = Message(
        conversation_id=conversation.id,
        tenant_id=tenant.id,
        direction=MessageDirection.INBOUND,
        content="Sim, está tudo certo.",
        ai_reasoning={"summary": "Owner confirmed", "detected_intent": "confirmation"},
        classification_score=0.95,
        raw_payload={"from": "5511999990070", "text": "Sim, está tudo certo."},
    )
    db_session.add(outbound_msg)
    db_session.add(inbound_msg)
    await db_session.commit()

    response = await client.get(f"/conversations/{conversation.id}")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(conversation.id)
    messages = data["messages"]
    assert len(messages) == 2

    # Verify inbound message has full audit fields
    inbound_data = next(m for m in messages if m["direction"] == "inbound")
    assert inbound_data["ai_reasoning"]["summary"] == "Owner confirmed"
    assert inbound_data["classification_score"] == pytest.approx(0.95)
    assert inbound_data["raw_payload"]["from"] == "5511999990070"

    # Verify outbound message (no AI reasoning but raw_payload present)
    outbound_data = next(m for m in messages if m["direction"] == "outbound")
    assert outbound_data["raw_payload"]["template"] == "data_refresh_request"


async def test_t24_conversation_detail_returns_404_for_missing_id(
    client: AsyncClient,
):
    """T24 edge case — Unknown conversation ID returns 404."""
    import uuid

    response = await client.get(f"/conversations/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_t24_conversation_detail_scoped_to_tenant(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    tenant_b: Tenant,
):
    """T24 edge case — Conversation belonging to another tenant returns 404."""
    batch = Batch(
        tenant_id=tenant_b.id,
        file_name="other.csv",
        file_key="test/other.csv",
        file_size=512,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant_b.id,
        row_number=1,
        phone_number="5511999990080",
        owner_name="Stranger",
        entity_type="property",
        original_data={"phone_number": "5511999990080", "owner_name": "Stranger"},
        status=BatchRecordStatus.OUTREACH,
    )
    db_session.add(record)
    await db_session.flush()

    conversation = Conversation(
        batch_record_id=record.id,
        tenant_id=tenant_b.id,  # different tenant
        phone_number="5511999990080",
        status=ConversationStatus.IN_PROGRESS,
    )
    db_session.add(conversation)
    await db_session.commit()

    # client is authenticated as admin_user belonging to tenant (not tenant_b)
    response = await client.get(f"/conversations/{conversation.id}")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# T25: Data erasure deletes all records for phone across batches
# AC-Audit-2: erase_data_for_phone must delete Messages, Conversations,
#              BatchRecords, and OptOutList entries for the given phone.
# ---------------------------------------------------------------------------


async def test_t25_erase_data_for_phone_deletes_all_related_rows(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T25 — AC-Audit-2: LGPD erasure removes all data for a phone across multiple batches."""
    target_phone = "5511999990090"

    # Create two batches with records for the same phone number
    batch_ids = []
    record_ids = []
    conversation_ids = []

    for i in range(2):
        batch = Batch(
            tenant_id=tenant.id,
            file_name=f"erase_batch_{i}.csv",
            file_key=f"test/erase_batch_{i}.csv",
            file_size=512,
            status=BatchStatus.COMPLETED,
        )
        db_session.add(batch)
        await db_session.flush()
        batch_ids.append(batch.id)

        record = BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=1,
            phone_number=target_phone,
            owner_name="Vítima LGPD",
            entity_type="property",
            original_data={"phone_number": target_phone, "owner_name": "Vítima LGPD"},
            status=BatchRecordStatus.COMPLETED,
        )
        db_session.add(record)
        await db_session.flush()
        record_ids.append(record.id)

        conversation = Conversation(
            batch_record_id=record.id,
            tenant_id=tenant.id,
            phone_number=target_phone,
            status=ConversationStatus.COMPLETED,
        )
        db_session.add(conversation)
        await db_session.flush()
        conversation_ids.append(conversation.id)

        message = Message(
            conversation_id=conversation.id,
            tenant_id=tenant.id,
            direction=MessageDirection.INBOUND,
            content="dados pessoais",
        )
        db_session.add(message)

    await db_session.commit()

    result = await erase_data_for_phone(db_session, tenant.id, target_phone)

    assert result["messages_deleted"] == 2
    assert result["conversations_deleted"] == 2
    assert result["records_deleted"] == 2

    # Verify rows are actually gone from the database
    remaining_records = await db_session.execute(
        select(BatchRecord).where(
            BatchRecord.tenant_id == tenant.id,
            BatchRecord.phone_number == target_phone,
        )
    )
    assert remaining_records.scalars().all() == []

    remaining_convs = await db_session.execute(
        select(Conversation).where(
            Conversation.tenant_id == tenant.id,
            Conversation.phone_number == target_phone,
        )
    )
    assert remaining_convs.scalars().all() == []

    remaining_msgs = await db_session.execute(
        select(Message).where(Message.conversation_id.in_(conversation_ids))
    )
    assert remaining_msgs.scalars().all() == []


async def test_t25_erase_does_not_affect_other_phones(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T25 edge case — Erasure of one phone must not touch records for other phones."""
    target_phone = "5511999990091"
    safe_phone = "5511999990092"

    batch = Batch(
        tenant_id=tenant.id,
        file_name="safe.csv",
        file_key="test/safe.csv",
        file_size=512,
        status=BatchStatus.COMPLETED,
    )
    db_session.add(batch)
    await db_session.flush()

    for phone in (target_phone, safe_phone):
        record = BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=1,
            phone_number=phone,
            owner_name=f"Owner {phone}",
            entity_type="property",
            original_data={"phone_number": phone, "owner_name": f"Owner {phone}"},
            status=BatchRecordStatus.COMPLETED,
        )
        db_session.add(record)
    await db_session.commit()

    await erase_data_for_phone(db_session, tenant.id, target_phone)

    remaining = await db_session.execute(
        select(BatchRecord).where(
            BatchRecord.tenant_id == tenant.id,
            BatchRecord.phone_number == safe_phone,
        )
    )
    assert len(remaining.scalars().all()) == 1
