"""
Unit tests for opt-out enforcement during batch approval.

T27: AC-OptOut-1 — Opted-out phone is skipped when a batch is approved
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
    OptOutList,
    Tenant,
    TenantUser,
)


# ---------------------------------------------------------------------------
# T27: Opted-out phone skipped in future batches
# AC-OptOut-1: During batch approval, READY records whose phone_number appears
#              in OptOutList must NOT get a Conversation created.
# ---------------------------------------------------------------------------


async def test_t27_opted_out_phone_gets_no_conversation_on_approve(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T27 — AC-OptOut-1: Opted-out phone is excluded from conversation creation on approval."""
    opted_out_phone = "5511999990001"
    normal_phone = "5511999990002"

    # Register the opt-out for the first phone
    opt_out = OptOutList(
        tenant_id=tenant.id,
        phone_number=opted_out_phone,
        reason="user requested removal",
    )
    db_session.add(opt_out)

    batch = Batch(
        tenant_id=tenant.id,
        file_name="optout.csv",
        file_key="test/optout.csv",
        file_size=1024,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    # Column mappings so the approval gate does not block on unmapped columns
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="phone_number",
            friendly_name="Telefone",
        )
    )
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="owner_name",
            friendly_name="Nome",
        )
    )

    opted_out_record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number=opted_out_phone,
        owner_name="Cliente Optado",
        entity_type="property",
        original_data={
            "phone_number": opted_out_phone,
            "owner_name": "Cliente Optado",
        },
        status=BatchRecordStatus.READY,
    )
    normal_record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=2,
        phone_number=normal_phone,
        owner_name="Cliente Normal",
        entity_type="property",
        original_data={
            "phone_number": normal_phone,
            "owner_name": "Cliente Normal",
        },
        status=BatchRecordStatus.READY,
    )
    db_session.add(opted_out_record)
    db_session.add(normal_record)
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 200
    assert response.json()["status"] == BatchStatus.APPROVED

    # Exactly ONE conversation should exist — only for the normal phone
    conversations = (
        (
            await db_session.execute(
                select(Conversation).where(Conversation.tenant_id == tenant.id)
            )
        )
        .scalars()
        .all()
    )

    assert len(conversations) == 1
    assert conversations[0].phone_number == normal_phone

    # The opted-out record should NOT have a conversation
    opted_out_convs = (
        (
            await db_session.execute(
                select(Conversation).where(
                    Conversation.phone_number == opted_out_phone,
                    Conversation.tenant_id == tenant.id,
                )
            )
        )
        .scalars()
        .all()
    )
    assert opted_out_convs == []


async def test_t27_approve_with_no_opt_outs_creates_all_conversations(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T27 edge case — When no opt-outs exist all READY records get Conversations."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="nooptout.csv",
        file_key="test/nooptout.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="phone_number",
            friendly_name="Telefone",
        )
    )
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="owner_name",
            friendly_name="Nome",
        )
    )

    for i in range(3):
        phone = f"551199999020{i}"
        db_session.add(
            BatchRecord(
                batch_id=batch.id,
                tenant_id=tenant.id,
                row_number=i + 1,
                phone_number=phone,
                owner_name=f"Owner {i}",
                entity_type="property",
                original_data={"phone_number": phone, "owner_name": f"Owner {i}"},
                status=BatchRecordStatus.READY,
            )
        )
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 200

    conversations = (
        (
            await db_session.execute(
                select(Conversation).where(Conversation.tenant_id == tenant.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(conversations) == 3


async def test_t27_all_records_opted_out_batch_still_approves(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
    admin_user: TenantUser,
):
    """T27 edge case — Batch where every record is opted-out approves with zero conversations."""
    opted_phone = "5511999990300"

    db_session.add(
        OptOutList(
            tenant_id=tenant.id,
            phone_number=opted_phone,
            reason="explicit request",
        )
    )

    batch = Batch(
        tenant_id=tenant.id,
        file_name="alloptout.csv",
        file_key="test/alloptout.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
    )
    db_session.add(batch)
    await db_session.flush()

    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="phone_number",
            friendly_name="Telefone",
        )
    )
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="owner_name",
            friendly_name="Nome",
        )
    )

    db_session.add(
        BatchRecord(
            batch_id=batch.id,
            tenant_id=tenant.id,
            row_number=1,
            phone_number=opted_phone,
            owner_name="Opted Owner",
            entity_type="property",
            original_data={"phone_number": opted_phone, "owner_name": "Opted Owner"},
            status=BatchRecordStatus.READY,
        )
    )
    await db_session.commit()

    response = await client.post(f"/batches/{batch.id}/approve")

    assert response.status_code == 200
    assert response.json()["status"] == BatchStatus.APPROVED

    conversations = (
        (
            await db_session.execute(
                select(Conversation).where(Conversation.tenant_id == tenant.id)
            )
        )
        .scalars()
        .all()
    )
    assert conversations == []
