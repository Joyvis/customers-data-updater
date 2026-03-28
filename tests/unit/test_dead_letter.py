"""
Unit tests for dead letter queue behaviour.

T22: AC-DL-1 — Dead letter records appear in dead letter endpoint
T23: AC-DL-2 — Batch with dead letters → PARTIALLY_COMPLETED
"""

import uuid

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    Conversation,
    ConversationStatus,
    Tenant,
    TenantUser,
)
from app.services.outreach import check_batch_completion


# ---------------------------------------------------------------------------
# T22: Dead letter records appear in the dead letter endpoint
# AC-DL-1: GET /batches/{batch_id}/dead-letter must return records in DEAD_LETTER
# ---------------------------------------------------------------------------


async def test_t22_dead_letter_records_appear_in_endpoint(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T22 — AC-DL-1: DEAD_LETTER records are returned by the dead-letter endpoint."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="dl_test.csv",
        file_key="test/dl_test.csv",
        file_size=1024,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990020",
        owner_name="Ana Paula",
        entity_type="property",
        original_data={"phone_number": "5511999990020", "owner_name": "Ana Paula"},
        status=BatchRecordStatus.DEAD_LETTER,
    )
    db_session.add(record)
    await db_session.flush()

    # Attach a FAILED conversation so the reason can be determined
    conversation = Conversation(
        batch_record_id=record.id,
        tenant_id=tenant.id,
        phone_number="5511999990020",
        status=ConversationStatus.FAILED,
        message_count=5,
        max_messages=5,
    )
    db_session.add(conversation)
    await db_session.commit()

    response = await client.get(f"/batches/{batch.id}/dead-letter")

    assert response.status_code == 200
    items = response.json()
    assert len(items) == 1
    assert items[0]["phone_number"] == "5511999990020"
    assert items[0]["status"] == BatchRecordStatus.DEAD_LETTER
    # Reason derived from the FAILED conversation
    assert items[0]["reason"] == "failed"


async def test_t22_dead_letter_endpoint_excludes_non_dead_letter_records(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T22 edge case — Only DEAD_LETTER records appear; COMPLETED records are excluded."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="mixed.csv",
        file_key="test/mixed.csv",
        file_size=1024,
        status=BatchStatus.COMPLETED,
    )
    db_session.add(batch)
    await db_session.flush()

    completed_record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990030",
        owner_name="Luiz Faria",
        entity_type="property",
        original_data={"phone_number": "5511999990030", "owner_name": "Luiz Faria"},
        status=BatchRecordStatus.COMPLETED,
    )
    dead_record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=2,
        phone_number="5511999990031",
        owner_name="Rita Souza",
        entity_type="property",
        original_data={"phone_number": "5511999990031", "owner_name": "Rita Souza"},
        status=BatchRecordStatus.DEAD_LETTER,
    )
    db_session.add(completed_record)
    db_session.add(dead_record)
    await db_session.commit()

    response = await client.get(f"/batches/{batch.id}/dead-letter")

    assert response.status_code == 200
    items = response.json()
    assert len(items) == 1
    assert items[0]["phone_number"] == "5511999990031"


async def test_t22_dead_letter_endpoint_returns_404_for_unknown_batch(
    client: AsyncClient,
):
    """T22 edge case — Non-existent batch returns 404."""
    response = await client.get(f"/batches/{uuid.uuid4()}/dead-letter")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# T23: Batch with dead letters → PARTIALLY_COMPLETED
# AC-DL-2: check_batch_completion sets PARTIALLY_COMPLETED when some records fail
# ---------------------------------------------------------------------------


async def test_t23_batch_partially_completed_when_some_dead_letters(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T23 — AC-DL-2: Batch with mixed COMPLETED/DEAD_LETTER records → PARTIALLY_COMPLETED."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="partial.csv",
        file_key="test/partial.csv",
        file_size=1024,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    records = [
        BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=1,
            phone_number="5511999990040",
            owner_name="Paula Rodrigues",
            entity_type="property",
            original_data={
                "phone_number": "5511999990040",
                "owner_name": "Paula Rodrigues",
            },
            status=BatchRecordStatus.COMPLETED,
        ),
        BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=2,
            phone_number="5511999990041",
            owner_name="Marcos Costa",
            entity_type="property",
            original_data={
                "phone_number": "5511999990041",
                "owner_name": "Marcos Costa",
            },
            status=BatchRecordStatus.DEAD_LETTER,
        ),
    ]
    for r in records:
        db_session.add(r)
    await db_session.commit()

    await check_batch_completion(db_session, batch.id)

    await db_session.refresh(batch)
    assert batch.status == BatchStatus.PARTIALLY_COMPLETED


async def test_t23_batch_completed_when_all_records_completed(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T23 edge case — All COMPLETED records → batch status becomes COMPLETED."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="allcomplete.csv",
        file_key="test/allcomplete.csv",
        file_size=512,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    for i in range(3):
        record = BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=i + 1,
            phone_number=f"551199999{9000 + i}",
            owner_name=f"Owner {i}",
            entity_type="property",
            original_data={
                "phone_number": f"551199999{9000 + i}",
                "owner_name": f"Owner {i}",
            },
            status=BatchRecordStatus.COMPLETED,
        )
        db_session.add(record)
    await db_session.commit()

    await check_batch_completion(db_session, batch.id)

    await db_session.refresh(batch)
    assert batch.status == BatchStatus.COMPLETED


async def test_t23_batch_status_unchanged_while_records_still_in_progress(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T23 edge case — Non-terminal records keep batch status unchanged."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="inprogress.csv",
        file_key="test/inprogress.csv",
        file_size=512,
        status=BatchStatus.OUTREACH,
    )
    db_session.add(batch)
    await db_session.flush()

    # One COMPLETED, one still in OUTREACH (non-terminal)
    db_session.add(
        BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=1,
            phone_number="5511999990060",
            owner_name="Gabriel Alves",
            entity_type="property",
            original_data={
                "phone_number": "5511999990060",
                "owner_name": "Gabriel Alves",
            },
            status=BatchRecordStatus.COMPLETED,
        )
    )
    db_session.add(
        BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=2,
            phone_number="5511999990061",
            owner_name="Carla Neves",
            entity_type="property",
            original_data={
                "phone_number": "5511999990061",
                "owner_name": "Carla Neves",
            },
            status=BatchRecordStatus.OUTREACH,
        )
    )
    await db_session.commit()

    await check_batch_completion(db_session, batch.id)

    await db_session.refresh(batch)
    # Should remain OUTREACH because not all records are terminal
    assert batch.status == BatchStatus.OUTREACH
