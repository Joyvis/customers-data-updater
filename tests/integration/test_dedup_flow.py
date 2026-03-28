"""
Integration tests: IT5 — Deduplication flow: upload batch with duplicates → review → resolve → approve.

Acceptance criteria covered:
  AC-IT5.1: Processing a batch with duplicate phone+entity_type rows marks them as DEDUP_REVIEW
  AC-IT5.2: GET /batches/{id}/dedup-groups returns exactly 1 group containing the 2 duplicate records
  AC-IT5.3: POST /batches/{id}/dedup-groups/{group_id}/resolve with merge action sets
             primary → READY and secondary → SKIPPED
  AC-IT5.4: After dedup resolution, POST /batches/{id}/approve succeeds (returns 200)
  AC-IT5.5: Approving without resolving dedup groups returns 409
"""

import io
import uuid
from unittest.mock import patch

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    ColumnMapping,
    Tenant,
)
from app.services.processing import process_batch


def _make_csv_with_duplicates() -> bytes:
    """5 rows: rows 1 & 2 share the same phone_number + entity_type (property)."""
    lines = [
        "phone_number,owner_name,address,status",
        "5511999990001,Alice Silva,Rua A 1,active",  # duplicate A
        "5511999990001,Alice Silva Jr,Rua A 2,active",  # duplicate A (same phone)
        "5511999990002,Bob Souza,Rua B 1,inactive",
        "5511999990003,Carol Lima,Rua C 1,active",
        "5511999990004,Dave Pinto,Rua D 1,inactive",
    ]
    return "\n".join(lines).encode("utf-8")


async def _upload_and_process(
    client: AsyncClient,
    db_session: AsyncSession,
    csv_bytes: bytes,
) -> uuid.UUID:
    """Helper: upload CSV and run process_batch; return batch_id."""
    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("dedup.csv", io.BytesIO(csv_bytes), "text/csv")},
        )
    assert response.status_code == 201, response.text
    batch_id = uuid.UUID(response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    return batch_id


# ---------------------------------------------------------------------------
# IT5.1 — Duplicates are flagged as DEDUP_REVIEW during processing
# ---------------------------------------------------------------------------


async def test_it5_duplicate_records_have_dedup_review_status(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT5.1: BatchRecords with duplicate phone+entity_type receive status DEDUP_REVIEW."""
    csv_bytes = _make_csv_with_duplicates()
    batch_id = await _upload_and_process(client, db_session, csv_bytes)

    result = await db_session.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.status == BatchRecordStatus.DEDUP_REVIEW,
        )
    )
    dedup_records = result.scalars().all()
    assert len(dedup_records) == 2
    # Both records share the duplicated phone number
    phone_numbers = {r.phone_number for r in dedup_records}
    assert phone_numbers == {"5511999990001"}


# ---------------------------------------------------------------------------
# IT5.2 — GET /dedup-groups returns exactly 1 group with 2 records
# ---------------------------------------------------------------------------


async def test_it5_dedup_groups_endpoint_returns_one_group_with_two_records(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT5.2: GET /batches/{id}/dedup-groups returns 1 group containing the 2 duplicate records."""
    csv_bytes = _make_csv_with_duplicates()
    batch_id = await _upload_and_process(client, db_session, csv_bytes)

    response = await client.get(f"/batches/{batch_id}/dedup-groups")
    assert response.status_code == 200, response.text

    groups = response.json()
    assert len(groups) == 1

    group = groups[0]
    assert "dedup_group_id" in group
    assert len(group["records"]) == 2


# ---------------------------------------------------------------------------
# IT5.3 — Resolve with merge sets primary=READY, secondary=SKIPPED
# ---------------------------------------------------------------------------


async def test_it5_resolve_merge_sets_primary_ready_and_secondary_skipped(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT5.3: Resolving a dedup group with merge action marks primary READY, others SKIPPED."""
    csv_bytes = _make_csv_with_duplicates()
    batch_id = await _upload_and_process(client, db_session, csv_bytes)

    groups_response = await client.get(f"/batches/{batch_id}/dedup-groups")
    group = groups_response.json()[0]
    group_id = group["dedup_group_id"]
    primary_id = group["records"][0]["id"]

    resolve_response = await client.post(
        f"/batches/{batch_id}/dedup-groups/{group_id}/resolve",
        json={"action": "merge", "primary_record_id": primary_id},
    )
    assert resolve_response.status_code == 200, resolve_response.text

    records = resolve_response.json()
    statuses = {r["id"]: r["status"] for r in records}

    assert statuses[primary_id] == BatchRecordStatus.READY
    secondary_ids = [r["id"] for r in records if r["id"] != primary_id]
    for sid in secondary_ids:
        assert statuses[sid] == BatchRecordStatus.SKIPPED


# ---------------------------------------------------------------------------
# IT5.4 — After dedup resolution + column mappings, approval succeeds
# ---------------------------------------------------------------------------


async def test_it5_approve_succeeds_after_dedup_resolution(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT5.4: POST /batches/{id}/approve returns 200 when all dedup groups are resolved."""
    csv_bytes = _make_csv_with_duplicates()
    batch_id = await _upload_and_process(client, db_session, csv_bytes)

    # Resolve the dedup group
    groups_response = await client.get(f"/batches/{batch_id}/dedup-groups")
    group = groups_response.json()[0]
    group_id = group["dedup_group_id"]
    primary_id = group["records"][0]["id"]

    await client.post(
        f"/batches/{batch_id}/dedup-groups/{group_id}/resolve",
        json={"action": "merge", "primary_record_id": primary_id},
    )

    # The approval router checks ColumnMapping for ALL original_data keys, including
    # phone_number and owner_name, so we must register every CSV column.
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

    approve_response = await client.post(f"/batches/{batch_id}/approve")
    assert approve_response.status_code == 200, approve_response.text
    assert approve_response.json()["status"] == BatchStatus.APPROVED


# ---------------------------------------------------------------------------
# IT5.5 — Approving without resolving dedup groups returns 409
# ---------------------------------------------------------------------------


async def test_it5_approve_blocked_when_dedup_unresolved(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT5.5: POST /batches/{id}/approve returns 409 if unresolved dedup groups exist."""
    csv_bytes = _make_csv_with_duplicates()
    batch_id = await _upload_and_process(client, db_session, csv_bytes)

    # Do NOT resolve dedup groups — approve should fail
    approve_response = await client.post(f"/batches/{batch_id}/approve")
    assert approve_response.status_code == 409
    assert "dedup" in approve_response.text.lower()
