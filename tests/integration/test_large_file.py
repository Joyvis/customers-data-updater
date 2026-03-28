"""
Integration tests: IT4 — Large file upload (500 rows), chunked processing.

Acceptance criteria covered:
  AC-IT4.1: A 500-row CSV can be uploaded and processed without errors
  AC-IT4.2: batch.total_records == 500 after processing
  AC-IT4.3: batch.processed_records == 500 after processing
  AC-IT4.4: Exactly 500 BatchRecord rows exist in the DB for this batch
  AC-IT4.5: Batch status is REVIEW after processing completes
"""

import io
import uuid
from unittest.mock import patch

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Batch, BatchRecord, BatchStatus, Tenant


def _make_large_csv(num_rows: int) -> bytes:
    """Generate a CSV with `num_rows` unique phone numbers."""
    lines = ["phone_number,owner_name,address,status"]
    for i in range(num_rows):
        phone = f"55119{i:07d}"  # unique 12-digit number per row
        lines.append(f"{phone},Owner {i},Rua {i} Street,active")
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# IT4.1–5 — Upload + process 500 rows
# ---------------------------------------------------------------------------


async def test_it4_upload_500_rows_succeeds(
    client: AsyncClient,
    tenant: Tenant,
) -> None:
    """AC-IT4.1: Uploading a 500-row CSV via POST /batches returns HTTP 201."""
    csv_bytes = _make_large_csv(500)

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("large.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    assert response.status_code == 201, response.text


async def test_it4_process_500_rows_sets_total_and_processed_records(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT4.2 & AC-IT4.3: total_records and processed_records are both 500 after processing."""
    from app.services.processing import process_batch

    csv_bytes = _make_large_csv(500)

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("large.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    batch_id = uuid.UUID(response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    result = await db_session.execute(select(Batch).where(Batch.id == batch_id))
    batch = result.scalar_one()

    assert batch.total_records == 500
    assert batch.processed_records == 500


async def test_it4_process_500_rows_creates_500_batch_records(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT4.4: Exactly 500 BatchRecord entries exist after processing a 500-row CSV."""
    from app.services.processing import process_batch

    csv_bytes = _make_large_csv(500)

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("large.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    batch_id = uuid.UUID(response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    count_result = await db_session.execute(
        select(func.count()).where(BatchRecord.batch_id == batch_id)
    )
    record_count = count_result.scalar_one()

    assert record_count == 500


async def test_it4_batch_status_is_review_after_processing(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT4.5: Batch status is REVIEW after process_batch() completes on 500 rows."""
    from app.services.processing import process_batch

    csv_bytes = _make_large_csv(500)

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("large.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    batch_id = uuid.UUID(response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    get_response = await client.get(f"/batches/{batch_id}")
    assert get_response.status_code == 200
    assert get_response.json()["status"] == BatchStatus.REVIEW
