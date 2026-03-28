"""
Unit tests for batch review and approval endpoints.

T16: AC-Batch-Review-1 — Approve batch with unresolved dedup → 409 rejection
T17: AC-Batch-Review-2 — Approve clean batch → APPROVED + Conversations created
"""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    ColumnMapping,
    Conversation,
    Tenant,
    TenantUser,
)


# ---------------------------------------------------------------------------
# T16: Approve batch with unresolved dedup groups → 409 Conflict
# AC: Approval must be blocked when any BatchRecord remains in DEDUP_REVIEW
# ---------------------------------------------------------------------------


async def test_t16_approve_batch_with_unresolved_dedup_returns_409(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T16 — AC-Batch-Review-1: Batch with DEDUP_REVIEW records cannot be approved."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="test.csv",
        file_key="test/test.csv",
        file_size=1024,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    # Create two records in DEDUP_REVIEW with a dedup_group_id
    record_a = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990001",
        owner_name="João Silva",
        entity_type="property",
        original_data={"phone_number": "5511999990001", "owner_name": "João Silva"},
        status=BatchRecordStatus.DEDUP_REVIEW,
        dedup_group_id="group-001",
    )
    record_b = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=2,
        phone_number="5511999990001",
        owner_name="João Silva",
        entity_type="property",
        original_data={"phone_number": "5511999990001", "owner_name": "João Silva"},
        status=BatchRecordStatus.DEDUP_REVIEW,
        dedup_group_id="group-001",
    )
    db_session.add(record_a)
    db_session.add(record_b)
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "dedup" in detail.lower()


# ---------------------------------------------------------------------------
# T17: Approve clean batch → APPROVED + Conversations created
# AC: A REVIEW batch with all READY records and mapped columns must be approved
#     and Conversation rows created for each READY record.
# ---------------------------------------------------------------------------


async def test_t17_approve_clean_batch_creates_conversations(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T17 — AC-Batch-Review-2: Clean batch transitions to APPROVED and creates Conversations."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="clean.csv",
        file_key="test/clean.csv",
        file_size=2048,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    # Create column mappings so the approval gate does not block on unmapped cols
    phone_mapping = ColumnMapping(
        tenant_id=tenant.id,
        entity_type="property",
        original_name="phone_number",
        friendly_name="Telefone",
    )
    name_mapping = ColumnMapping(
        tenant_id=tenant.id,
        entity_type="property",
        original_name="owner_name",
        friendly_name="Nome do Proprietário",
    )
    db_session.add(phone_mapping)
    db_session.add(name_mapping)

    record_1 = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990001",
        owner_name="João Silva",
        entity_type="property",
        original_data={"phone_number": "5511999990001", "owner_name": "João Silva"},
        status=BatchRecordStatus.READY,
    )
    record_2 = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=2,
        phone_number="5511999990002",
        owner_name="Maria Santos",
        entity_type="property",
        original_data={"phone_number": "5511999990002", "owner_name": "Maria Santos"},
        status=BatchRecordStatus.READY,
    )
    db_session.add(record_1)
    db_session.add(record_2)
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == BatchStatus.APPROVED

    # Verify Conversation rows were created for both records
    conv_result = await db_session.execute(
        select(Conversation).where(Conversation.tenant_id == tenant.id)
    )
    conversations = conv_result.scalars().all()
    assert len(conversations) == 2

    conv_phones = {c.phone_number for c in conversations}
    assert "5511999990001" in conv_phones
    assert "5511999990002" in conv_phones


async def test_t17_approve_batch_not_in_review_returns_409(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T17 edge case — Batch in non-REVIEW status cannot be approved."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="queued.csv",
        file_key="test/queued.csv",
        file_size=512,
        status=BatchStatus.QUEUED,
    )
    db_session.add(batch)
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 409


async def test_t17_approve_batch_with_unmapped_columns_returns_409(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T17 edge case — Batch with unmapped columns cannot be approved."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="unmapped.csv",
        file_key="test/unmapped.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    # Record has columns with no ColumnMapping entry
    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990003",
        owner_name="Carlos Mendes",
        entity_type="property",
        original_data={
            "phone_number": "5511999990003",
            "owner_name": "Carlos Mendes",
            "mystery_column": "some_value",
        },
        status=BatchRecordStatus.READY,
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 409
