"""
Unit tests for app/services/processing.py

Coverage:
  T6 - AC: process_batch creates correct BatchRecord entries when processing rows in chunks
  T7 - AC: process_batch sets batch.processed_records equal to total row count after completion
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import Batch, BatchRecord, BatchRecordStatus, BatchStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_csv_rows(n: int) -> tuple[list[str], list[dict]]:
    """Return (headers, rows) for n simple property rows."""
    headers = ["phone_number", "owner_name", "address", "status", "type"]
    rows = [
        {
            "phone_number": f"1199999{i:04d}",
            "owner_name": f"Owner {i}",
            "address": f"Rua {i}",
            "status": "active",
            "type": "property",
        }
        for i in range(1, n + 1)
    ]
    return headers, rows


def _make_batch(tenant_id: uuid.UUID) -> Batch:
    return Batch(
        tenant_id=tenant_id,
        file_name="upload.csv",
        file_key="tenants/test/upload.csv",
        file_size=1024,
        status=BatchStatus.UPLOADED,
        settings={},
    )


# ---------------------------------------------------------------------------
# T6: Process batch with 5 rows (chunk_size=2) — records created correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_batch_creates_correct_records(db_session: AsyncSession, tenant):
    """
    T6 — process_batch creates one BatchRecord per row and transitions batch to REVIEW.

    Mocks:
      - storage.download_file  → returns dummy CSV bytes
      - file_parser.parse_file → returns 5 prepared rows
      - file_parser.validate_required_columns → no-op
      - column_mapper.auto_map_columns → returns ({}, [])
      - entity_validator.validate_entity_types → returns []
    chunk_size is patched to 2 to exercise chunked processing.
    """
    from app.services.processing import process_batch

    batch = _make_batch(tenant.id)
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    headers, rows = _make_csv_rows(5)
    dummy_content = b"irrelevant_bytes"

    with (
        patch(
            "app.services.processing.storage.download_file", return_value=dummy_content
        ),
        patch(
            "app.services.processing.file_parser.parse_file",
            return_value=(headers, rows),
        ),
        patch("app.services.processing.file_parser.validate_required_columns"),
        patch(
            "app.services.processing.column_mapper.auto_map_columns",
            new=AsyncMock(return_value=({}, [])),
        ),
        patch(
            "app.services.processing.entity_validator.validate_entity_types",
            new=AsyncMock(return_value=[]),
        ),
        patch("app.services.processing.settings.chunk_size", 2),
    ):
        await process_batch(db_session, batch.id)

    # Reload batch after processing
    result = await db_session.execute(select(Batch).where(Batch.id == batch.id))
    refreshed_batch = result.scalar_one()

    assert refreshed_batch.status == BatchStatus.REVIEW
    assert refreshed_batch.total_records == 5

    # Verify all 5 records were created
    records_result = await db_session.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch.id)
    )
    records = list(records_result.scalars().all())
    assert len(records) == 5

    # All records should have the correct tenant
    for rec in records:
        assert rec.tenant_id == tenant.id
        assert rec.batch_id == batch.id

    # Row numbers 1..5 should all be present
    row_numbers = sorted(rec.row_number for rec in records)
    assert row_numbers == [1, 2, 3, 4, 5]

    # Phone numbers should match the input rows
    phones = {rec.phone_number for rec in records}
    expected_phones = {row["phone_number"] for row in rows}
    assert phones == expected_phones


@pytest.mark.asyncio
async def test_process_batch_records_have_correct_entity_type(
    db_session: AsyncSession, tenant
):
    """T6 (field check) — entity_type is extracted correctly from the 'type' column."""
    from app.services.processing import process_batch

    batch = _make_batch(tenant.id)
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    headers, rows = _make_csv_rows(2)
    dummy_content = b"irrelevant_bytes"

    with (
        patch(
            "app.services.processing.storage.download_file", return_value=dummy_content
        ),
        patch(
            "app.services.processing.file_parser.parse_file",
            return_value=(headers, rows),
        ),
        patch("app.services.processing.file_parser.validate_required_columns"),
        patch(
            "app.services.processing.column_mapper.auto_map_columns",
            new=AsyncMock(return_value=({}, [])),
        ),
        patch(
            "app.services.processing.entity_validator.validate_entity_types",
            new=AsyncMock(return_value=[]),
        ),
    ):
        await process_batch(db_session, batch.id)

    records_result = await db_session.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch.id)
    )
    records = list(records_result.scalars().all())

    for rec in records:
        assert rec.entity_type == "property"


@pytest.mark.asyncio
async def test_process_batch_non_dedup_records_are_ready(
    db_session: AsyncSession, tenant
):
    """
    T6 (status fix) — Non-duplicate PENDING records must be READY after processing,
    not left in PENDING status (post-fix behaviour from AC5.2 implementation).
    """
    from app.services.processing import process_batch

    batch = _make_batch(tenant.id)
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    # All unique phone numbers → no duplicates
    headers, rows = _make_csv_rows(3)
    dummy_content = b"irrelevant_bytes"

    with (
        patch(
            "app.services.processing.storage.download_file", return_value=dummy_content
        ),
        patch(
            "app.services.processing.file_parser.parse_file",
            return_value=(headers, rows),
        ),
        patch("app.services.processing.file_parser.validate_required_columns"),
        patch(
            "app.services.processing.column_mapper.auto_map_columns",
            new=AsyncMock(return_value=({}, [])),
        ),
        patch(
            "app.services.processing.entity_validator.validate_entity_types",
            new=AsyncMock(return_value=[]),
        ),
    ):
        await process_batch(db_session, batch.id)

    records_result = await db_session.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch.id)
    )
    records = list(records_result.scalars().all())

    # Every non-dedup record must be READY, not PENDING
    for rec in records:
        assert rec.status == BatchRecordStatus.READY, (
            f"Record row {rec.row_number} expected READY but got {rec.status}"
        )


# ---------------------------------------------------------------------------
# T7: Progress tracking — processed_records == total_records after processing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_batch_processed_records_equals_total(
    db_session: AsyncSession, tenant
):
    """
    T7 — After process_batch completes, batch.processed_records equals batch.total_records
    which equals the number of rows in the file.
    """
    from app.services.processing import process_batch

    batch = _make_batch(tenant.id)
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    headers, rows = _make_csv_rows(5)
    dummy_content = b"irrelevant_bytes"

    with (
        patch(
            "app.services.processing.storage.download_file", return_value=dummy_content
        ),
        patch(
            "app.services.processing.file_parser.parse_file",
            return_value=(headers, rows),
        ),
        patch("app.services.processing.file_parser.validate_required_columns"),
        patch(
            "app.services.processing.column_mapper.auto_map_columns",
            new=AsyncMock(return_value=({}, [])),
        ),
        patch(
            "app.services.processing.entity_validator.validate_entity_types",
            new=AsyncMock(return_value=[]),
        ),
    ):
        await process_batch(db_session, batch.id)

    result = await db_session.execute(select(Batch).where(Batch.id == batch.id))
    refreshed_batch = result.scalar_one()

    assert refreshed_batch.processed_records == len(rows)
    assert refreshed_batch.processed_records == refreshed_batch.total_records


@pytest.mark.asyncio
async def test_process_batch_stores_column_mapping_in_settings(
    db_session: AsyncSession, tenant
):
    """
    T7 (settings) — batch.settings stores column_mapping and unmapped_columns after processing.
    """
    from app.services.processing import process_batch

    batch = _make_batch(tenant.id)
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    headers, rows = _make_csv_rows(2)
    mapped_result = {"phone_number": "phone_number", "owner_name": "owner_name"}
    unmapped_result = ["address", "status", "type"]
    dummy_content = b"irrelevant_bytes"

    with (
        patch(
            "app.services.processing.storage.download_file", return_value=dummy_content
        ),
        patch(
            "app.services.processing.file_parser.parse_file",
            return_value=(headers, rows),
        ),
        patch("app.services.processing.file_parser.validate_required_columns"),
        patch(
            "app.services.processing.column_mapper.auto_map_columns",
            new=AsyncMock(return_value=(mapped_result, unmapped_result)),
        ),
        patch(
            "app.services.processing.entity_validator.validate_entity_types",
            new=AsyncMock(return_value=[]),
        ),
    ):
        await process_batch(db_session, batch.id)

    result = await db_session.execute(select(Batch).where(Batch.id == batch.id))
    refreshed_batch = result.scalar_one()

    assert "column_mapping" in refreshed_batch.settings
    assert "unmapped_columns" in refreshed_batch.settings
    assert refreshed_batch.settings["column_mapping"] == mapped_result
    assert refreshed_batch.settings["unmapped_columns"] == unmapped_result
