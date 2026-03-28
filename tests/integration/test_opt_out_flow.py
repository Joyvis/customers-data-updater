"""
Integration tests: IT8 — Opt-out flow.

Acceptance criteria covered:
  AC-IT8.1: When AI returns classification="opt_out", conversation is CANCELLED and
             BatchRecord becomes DEAD_LETTER
  AC-IT8.2: An OptOutList entry is created for the opted-out phone number
  AC-IT8.3: A new batch containing the opted-out phone is approved without creating a
             Conversation for that phone (the record is skipped)
  AC-IT8.4: Other records in the new batch DO receive Conversations
"""

import io
import uuid
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from httpx import AsyncClient

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    ColumnMapping,
    Conversation,
    ConversationStatus,
    OptOutList,
    Tenant,
)
from app.services.outreach import process_inbound_message
from app.services.processing import process_batch


OPT_OUT_PHONE = "5511900000001"
NORMAL_PHONE = "5511900000002"


async def _create_batch_with_single_conversation(
    db_session: AsyncSession,
    tenant: Tenant,
    phone: str,
) -> tuple[Batch, BatchRecord, Conversation]:
    """Bootstrap an APPROVED Batch with a single OUTREACH record and IN_PROGRESS Conversation."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="data.csv",
        file_key=f"{tenant.id}/{uuid.uuid4()}.csv",
        file_size=512,
        status=BatchStatus.APPROVED,
        total_records=1,
        processed_records=1,
        max_messages_per_conversation=5,
    )
    db_session.add(batch)
    await db_session.flush()

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number=phone,
        owner_name="Opt Out Owner",
        entity_type="property",
        original_data={"phone_number": phone, "owner_name": "Opt Out Owner"},
        status=BatchRecordStatus.OUTREACH,
    )
    db_session.add(record)
    await db_session.flush()

    conv = Conversation(
        batch_record_id=record.id,
        tenant_id=tenant.id,
        phone_number=phone,
        status=ConversationStatus.IN_PROGRESS,
        message_count=1,
        max_messages=5,
    )
    db_session.add(conv)
    await db_session.commit()

    for obj in (batch, record, conv):
        await db_session.refresh(obj)

    return batch, record, conv


def _make_csv(phones: list[str]) -> bytes:
    lines = ["phone_number,owner_name,address,status"]
    for i, phone in enumerate(phones):
        lines.append(f"{phone},Owner {i},Rua {i},active")
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# IT8.1 — opt_out classification → CANCELLED conversation + DEAD_LETTER record
# ---------------------------------------------------------------------------


async def test_it8_opt_out_cancels_conversation_and_dead_letters_record(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT8.1: When AI returns 'opt_out', conversation becomes CANCELLED and record DEAD_LETTER."""
    _, record, conv = await _create_batch_with_single_conversation(
        db_session, tenant, OPT_OUT_PHONE
    )

    opt_out_result = {
        "classification": "opt_out",
        "updated_fields": None,
        "follow_up_message": "Tudo bem, não entraremos em contato.",
        "ai_reasoning": {},
        "classification_score": 0.99,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=opt_out_result,
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
            db_session, conv.id, "Não quero mais ser contactado", {"raw": True}
        )

    await db_session.refresh(conv)
    await db_session.refresh(record)

    assert conv.status == ConversationStatus.CANCELLED
    assert record.status == BatchRecordStatus.DEAD_LETTER


# ---------------------------------------------------------------------------
# IT8.2 — OptOutList entry is created for the phone number
# ---------------------------------------------------------------------------


async def test_it8_opt_out_creates_opt_out_list_entry(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT8.2: Processing an 'opt_out' response creates an OptOutList row for that phone."""
    _, record, conv = await _create_batch_with_single_conversation(
        db_session, tenant, OPT_OUT_PHONE
    )

    opt_out_result = {
        "classification": "opt_out",
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {},
        "classification_score": 0.99,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=opt_out_result,
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
            db_session, conv.id, "Para de me contatar", {"raw": True}
        )

    result = await db_session.execute(
        select(OptOutList).where(
            OptOutList.tenant_id == tenant.id,
            OptOutList.phone_number == OPT_OUT_PHONE,
        )
    )
    opt_out_entry = result.scalar_one_or_none()
    assert opt_out_entry is not None
    assert opt_out_entry.phone_number == OPT_OUT_PHONE


# ---------------------------------------------------------------------------
# IT8.3 — New batch approval skips opted-out phone number
# ---------------------------------------------------------------------------


async def test_it8_new_batch_does_not_create_conversation_for_opted_out_phone(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT8.3: When a new batch is approved, the opted-out phone does not get a Conversation."""
    # Step 1: Trigger opt-out for OPT_OUT_PHONE
    _, record, conv = await _create_batch_with_single_conversation(
        db_session, tenant, OPT_OUT_PHONE
    )

    opt_out_result = {
        "classification": "opt_out",
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {},
        "classification_score": 0.99,
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=opt_out_result,
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
            db_session, conv.id, "Remove meu número", {"raw": True}
        )

    # Confirm opt-out entry was created
    opt_out_check = await db_session.execute(
        select(OptOutList).where(
            OptOutList.tenant_id == tenant.id,
            OptOutList.phone_number == OPT_OUT_PHONE,
        )
    )
    assert opt_out_check.scalar_one_or_none() is not None, (
        "Opt-out entry was not created"
    )

    # Step 2: Upload and process a new batch with the opted-out phone AND a normal phone
    csv_bytes = _make_csv([OPT_OUT_PHONE, NORMAL_PHONE])

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        upload_response = await client.post(
            "/batches",
            files={"file": ("new_batch.csv", io.BytesIO(csv_bytes), "text/csv")},
        )
    assert upload_response.status_code == 201
    new_batch_id = uuid.UUID(upload_response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, new_batch_id)

    # Step 3: Add column mappings — the approval router checks ALL original_data keys
    # (including the standard phone_number and owner_name) against ColumnMapping rows.
    for col_name in ("phone_number", "owner_name", "address", "status"):
        db_session.add(
            ColumnMapping(
                tenant_id=tenant.id,
                entity_type="property",
                original_name=col_name,
                friendly_name=col_name,
            )
        )
    await db_session.commit()

    # Step 4: Approve the new batch
    approve_response = await client.post(f"/batches/{new_batch_id}/approve")
    assert approve_response.status_code == 200, approve_response.text

    # Step 5: Verify that no Conversation was created for the opted-out phone in the new batch
    conversations_result = await db_session.execute(
        select(Conversation).where(
            Conversation.tenant_id == tenant.id,
            Conversation.phone_number == OPT_OUT_PHONE,
            # Exclude conversations from the first batch (created earlier in this test)
        )
    )
    all_convs_for_opted_out = conversations_result.scalars().all()
    new_batch_record_ids = [
        r.id
        for r in (
            await db_session.execute(
                select(BatchRecord).where(BatchRecord.batch_id == new_batch_id)
            )
        )
        .scalars()
        .all()
    ]
    new_batch_convs_for_opted_out = [
        c for c in all_convs_for_opted_out if c.batch_record_id in new_batch_record_ids
    ]
    assert len(new_batch_convs_for_opted_out) == 0, (
        f"Expected no conversation for opted-out phone {OPT_OUT_PHONE} in new batch, "
        f"but found {len(new_batch_convs_for_opted_out)}"
    )


# ---------------------------------------------------------------------------
# IT8.4 — Non-opted-out phones in the same batch DO receive Conversations
# ---------------------------------------------------------------------------


async def test_it8_new_batch_creates_conversation_for_non_opted_out_phone(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT8.4: Phones that are NOT opted-out still receive Conversations upon approval."""
    # Create an opt-out entry directly (no need for a full conversation)
    db_session.add(
        OptOutList(
            tenant_id=tenant.id,
            phone_number=OPT_OUT_PHONE,
            reason="direct test entry",
        )
    )
    await db_session.commit()

    csv_bytes = _make_csv([OPT_OUT_PHONE, NORMAL_PHONE])

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        upload_response = await client.post(
            "/batches",
            files={"file": ("batch.csv", io.BytesIO(csv_bytes), "text/csv")},
        )
    batch_id = uuid.UUID(upload_response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    for col_name in ("phone_number", "owner_name", "address", "status"):
        db_session.add(
            ColumnMapping(
                tenant_id=tenant.id,
                entity_type="property",
                original_name=col_name,
                friendly_name=col_name,
            )
        )
    await db_session.commit()

    await client.post(f"/batches/{batch_id}/approve")

    convs_result = await db_session.execute(
        select(Conversation).where(
            Conversation.tenant_id == tenant.id,
            Conversation.phone_number == NORMAL_PHONE,
        )
    )
    normal_convs = convs_result.scalars().all()
    assert len(normal_convs) >= 1, (
        f"Expected at least 1 Conversation for {NORMAL_PHONE}, got {len(normal_convs)}"
    )
