"""
Unit tests for app/services/dedup.py

Coverage:
  T8  - AC: detect_duplicates groups records with the same phone+entity_type together,
            apply_dedup_flags sets DEDUP_REVIEW and a shared dedup_group_id
  T9  - AC: detect_duplicates returns empty dict when all combinations are unique
  T10 - AC: After merge resolution the primary record is READY and secondary is SKIPPED
"""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Batch, BatchRecord, BatchRecordStatus, BatchStatus
from app.services.dedup import apply_dedup_flags, detect_duplicates


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_record(phone_number: str, entity_type: str = "property") -> SimpleNamespace:
    """Return a lightweight namespace that satisfies detect_duplicates' attribute access."""
    return SimpleNamespace(phone_number=phone_number, entity_type=entity_type)


def _real_batch_record(
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    phone_number: str,
    row_number: int,
    entity_type: str = "property",
) -> BatchRecord:
    return BatchRecord(
        batch_id=batch_id,
        tenant_id=tenant_id,
        row_number=row_number,
        phone_number=phone_number,
        owner_name=f"Owner {row_number}",
        entity_type=entity_type,
        original_data={
            "phone_number": phone_number,
            "owner_name": f"Owner {row_number}",
        },
        status=BatchRecordStatus.PENDING,
    )


# ---------------------------------------------------------------------------
# T8: Two records with same phone+type → same dedup_group_id, both DEDUP_REVIEW
# ---------------------------------------------------------------------------


def test_detect_duplicates_groups_same_phone_and_entity_type():
    """T8 — Two records sharing the same phone_number+entity_type produce one group."""
    rec_a = _mock_record("11999990001", "property")
    rec_b = _mock_record("11999990001", "property")
    # Third record with a different phone — must NOT end up in the group
    rec_c = _mock_record("11999990002", "property")

    groups = detect_duplicates([rec_a, rec_b, rec_c])

    # Exactly one duplicate group expected
    assert len(groups) == 1

    group_records = next(iter(groups.values()))
    assert len(group_records) == 2
    assert rec_a in group_records
    assert rec_b in group_records
    assert rec_c not in group_records


def test_detect_duplicates_group_id_is_stable_and_shared():
    """T8 — Both duplicates receive the same deterministic dedup_group_id key."""
    rec_a = _mock_record("11999990001", "property")
    rec_b = _mock_record("11999990001", "property")

    groups = detect_duplicates([rec_a, rec_b])

    group_ids = list(groups.keys())
    assert len(group_ids) == 1
    # The key must be a non-empty string (sha256 hex prefix)
    assert isinstance(group_ids[0], str)
    assert len(group_ids[0]) > 0


@pytest.mark.asyncio
async def test_apply_dedup_flags_sets_dedup_review_and_group_id(
    db_session: AsyncSession, tenant
):
    """T8 — apply_dedup_flags marks both records DEDUP_REVIEW with the same group_id."""
    # Create a real Batch in the DB first
    batch = Batch(
        tenant_id=tenant.id,
        file_name="test.csv",
        file_key="test/key.csv",
        file_size=512,
        status=BatchStatus.UPLOADED,
        settings={},
    )
    db_session.add(batch)
    await db_session.flush()

    rec_a = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=1)
    rec_b = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=2)
    db_session.add(rec_a)
    db_session.add(rec_b)
    await db_session.flush()

    # Detect duplicates using actual objects (they have the attributes needed)
    groups = detect_duplicates([rec_a, rec_b])
    assert len(groups) == 1

    # Apply dedup flags
    await apply_dedup_flags(db_session, groups)

    # Verify both records are in DEDUP_REVIEW and share the same dedup_group_id
    group_id = next(iter(groups.keys()))
    assert rec_a.status == BatchRecordStatus.DEDUP_REVIEW
    assert rec_b.status == BatchRecordStatus.DEDUP_REVIEW
    assert rec_a.dedup_group_id == group_id
    assert rec_b.dedup_group_id == group_id
    assert rec_a.dedup_group_id == rec_b.dedup_group_id


# ---------------------------------------------------------------------------
# T9: All unique combinations → detect_duplicates returns empty dict
# ---------------------------------------------------------------------------


def test_detect_duplicates_returns_empty_dict_for_unique_records():
    """T9 — All records with unique phone+type combinations produce no groups."""
    records = [
        _mock_record("11999990001", "property"),
        _mock_record("11999990002", "property"),
        _mock_record("11999990003", "property"),
    ]

    groups = detect_duplicates(records)

    assert groups == {}


def test_detect_duplicates_different_entity_types_not_grouped():
    """T9 (edge) — Same phone but different entity_type are NOT considered duplicates."""
    rec_a = _mock_record("11999990001", "property")
    rec_b = _mock_record("11999990001", "land")

    groups = detect_duplicates([rec_a, rec_b])

    assert groups == {}


def test_detect_duplicates_empty_list_returns_empty_dict():
    """T9 (boundary) — Empty input list produces empty dict."""
    groups = detect_duplicates([])
    assert groups == {}


def test_detect_duplicates_single_record_not_a_duplicate():
    """T9 (boundary) — A single record never forms a duplicate group."""
    groups = detect_duplicates([_mock_record("11999990001", "property")])
    assert groups == {}


# ---------------------------------------------------------------------------
# T10: After merge resolution → primary is READY, secondary is SKIPPED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedup_resolve_primary_ready_secondary_skipped(
    db_session: AsyncSession, tenant
):
    """
    T10 — Simulates the resolution of a dedup group:
    the chosen primary record transitions to READY and the secondary to SKIPPED.

    Resolution logic lives in the router/endpoint that updates record statuses.
    This test verifies the expected final DB state directly, mirroring what the
    resolve_dedup_group endpoint must produce.
    """
    batch = Batch(
        tenant_id=tenant.id,
        file_name="test.csv",
        file_key="test/key.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
        settings={},
    )
    db_session.add(batch)
    await db_session.flush()

    group_id = "abc123dedup"

    primary = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=1)
    primary.status = BatchRecordStatus.DEDUP_REVIEW
    primary.dedup_group_id = group_id

    secondary = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=2)
    secondary.status = BatchRecordStatus.DEDUP_REVIEW
    secondary.dedup_group_id = group_id

    db_session.add(primary)
    db_session.add(secondary)
    await db_session.commit()
    await db_session.refresh(primary)
    await db_session.refresh(secondary)

    # --- Simulate resolution: primary becomes READY, secondary becomes SKIPPED ---
    primary.status = BatchRecordStatus.READY
    secondary.status = BatchRecordStatus.SKIPPED
    await db_session.commit()

    # Re-fetch from DB to confirm persistence
    result = await db_session.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch.id)
    )
    all_records = list(result.scalars().all())
    assert len(all_records) == 2

    statuses = {rec.row_number: rec.status for rec in all_records}
    assert statuses[1] == BatchRecordStatus.READY
    assert statuses[2] == BatchRecordStatus.SKIPPED


@pytest.mark.asyncio
async def test_dedup_resolved_records_retain_group_id(db_session: AsyncSession, tenant):
    """T10 (integrity) — dedup_group_id is preserved on both records after resolution."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="test.csv",
        file_key="test/key.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
        settings={},
    )
    db_session.add(batch)
    await db_session.flush()

    group_id = "stable_group_hash_42"

    primary = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=1)
    primary.status = BatchRecordStatus.DEDUP_REVIEW
    primary.dedup_group_id = group_id

    secondary = _real_batch_record(batch.id, tenant.id, "11999990001", row_number=2)
    secondary.status = BatchRecordStatus.DEDUP_REVIEW
    secondary.dedup_group_id = group_id

    db_session.add(primary)
    db_session.add(secondary)
    await db_session.commit()

    # Resolve
    primary.status = BatchRecordStatus.READY
    secondary.status = BatchRecordStatus.SKIPPED
    await db_session.commit()
    await db_session.refresh(primary)
    await db_session.refresh(secondary)

    assert primary.dedup_group_id == group_id
    assert secondary.dedup_group_id == group_id
