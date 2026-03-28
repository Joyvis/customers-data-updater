"""
Integration tests: IT1 — End-to-end batch upload → process → approve → conversations created.

Acceptance criteria covered:
  AC-IT1.1: POST /batches with valid CSV → 201 and batch_id returned
  AC-IT1.2: After process_batch() the batch status transitions to REVIEW
  AC-IT1.3: POST /batches/{id}/approve → 200 and batch status is APPROVED
  AC-IT1.4: One Conversation is created per READY BatchRecord after approval
  AC-IT1.5: Batch with missing required columns is rejected at upload (422)
"""

import io
import uuid
from unittest.mock import patch

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BatchRecord,
    BatchStatus,
    ColumnMapping,
    Conversation,
    Tenant,
)
from app.services.processing import process_batch


def _make_csv(rows: list[dict]) -> bytes:
    """Build a minimal CSV with phone_number, owner_name, address, status columns."""
    lines = ["phone_number,owner_name,address,status"]
    for row in rows:
        lines.append(
            f"{row['phone_number']},{row['owner_name']},{row['address']},{row['status']}"
        )
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# IT1 — Happy path: upload → process → approve → conversations exist
# ---------------------------------------------------------------------------


async def test_it1_upload_csv_returns_201_and_batch_id(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT1.1: POST /batches with a valid CSV file returns HTTP 201 and a batch_id."""
    csv_bytes = _make_csv(
        [
            {
                "phone_number": "5511999990001",
                "owner_name": "Alice Silva",
                "address": "Rua A 1",
                "status": "active",
            },
        ]
    )

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("test.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    assert response.status_code == 201, response.text
    data = response.json()
    assert "id" in data
    assert data["status"] == BatchStatus.UPLOADED


async def test_it1_process_batch_transitions_to_review(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT1.2: After calling process_batch() the batch status becomes REVIEW."""
    csv_bytes = _make_csv(
        [
            {
                "phone_number": "5511999990001",
                "owner_name": "Alice Silva",
                "address": "Rua A 1",
                "status": "active",
            },
            {
                "phone_number": "5511999990002",
                "owner_name": "Bob Souza",
                "address": "Rua B 2",
                "status": "inactive",
            },
            {
                "phone_number": "5511999990003",
                "owner_name": "Carol Lima",
                "address": "Rua C 3",
                "status": "active",
            },
        ]
    )

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        upload_response = await client.post(
            "/batches",
            files={"file": ("test.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    assert upload_response.status_code == 201
    batch_id = uuid.UUID(upload_response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    get_response = await client.get(f"/batches/{batch_id}")
    assert get_response.status_code == 200
    assert get_response.json()["status"] == BatchStatus.REVIEW


async def test_it1_approve_batch_returns_200_and_approved_status(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT1.3: POST /batches/{id}/approve → 200 with status APPROVED."""
    csv_bytes = _make_csv(
        [
            {
                "phone_number": "5511999990001",
                "owner_name": "Alice Silva",
                "address": "Rua A 1",
                "status": "active",
            },
            {
                "phone_number": "5511999990002",
                "owner_name": "Bob Souza",
                "address": "Rua B 2",
                "status": "inactive",
            },
            {
                "phone_number": "5511999990003",
                "owner_name": "Carol Lima",
                "address": "Rua C 3",
                "status": "active",
            },
        ]
    )

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        upload_response = await client.post(
            "/batches",
            files={"file": ("test.csv", io.BytesIO(csv_bytes), "text/csv")},
        )

    batch_id = uuid.UUID(upload_response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    # The approval endpoint checks ColumnMapping rows for ALL columns present in original_data.
    # Standard columns (phone_number, owner_name) are not automatically exempted by the router,
    # so we must include them alongside the non-standard ones.
    for col_name in ("phone_number", "owner_name", "address", "status"):
        mapping = ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name=col_name,
            friendly_name=col_name,
        )
        db_session.add(mapping)
    await db_session.commit()

    approve_response = await client.post(f"/batches/{batch_id}/approve")
    assert approve_response.status_code == 200, approve_response.text
    assert approve_response.json()["status"] == BatchStatus.APPROVED


async def test_it1_approval_creates_conversations_for_each_ready_record(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT1.4: One Conversation is created per READY BatchRecord upon approval."""
    csv_bytes = _make_csv(
        [
            {
                "phone_number": "5511999990001",
                "owner_name": "Alice Silva",
                "address": "Rua A 1",
                "status": "active",
            },
            {
                "phone_number": "5511999990002",
                "owner_name": "Bob Souza",
                "address": "Rua B 2",
                "status": "inactive",
            },
            {
                "phone_number": "5511999990003",
                "owner_name": "Carol Lima",
                "address": "Rua C 3",
                "status": "active",
            },
        ]
    )

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        upload_response = await client.post(
            "/batches",
            files={"file": ("test.csv", io.BytesIO(csv_bytes), "text/csv")},
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

    conversations_result = await db_session.execute(
        select(Conversation).where(
            Conversation.tenant_id == tenant.id,
        )
    )
    conversations = conversations_result.scalars().all()
    assert len(conversations) == 3

    phone_numbers = {c.phone_number for c in conversations}
    assert "5511999990001" in phone_numbers
    assert "5511999990002" in phone_numbers
    assert "5511999990003" in phone_numbers


async def test_it1_upload_csv_missing_required_columns_returns_422(
    client: AsyncClient,
    tenant: Tenant,
) -> None:
    """AC-IT1.5: Uploading a CSV without required columns returns HTTP 422."""
    bad_csv = b"address,status\nRua A 1,active\n"

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("bad.csv", io.BytesIO(bad_csv), "text/csv")},
        )

    assert response.status_code == 422
    assert "phone_number" in response.text or "owner_name" in response.text


async def test_it1_batch_records_created_match_csv_row_count(
    client: AsyncClient,
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT1.2 (extra): The number of BatchRecords equals the number of CSV data rows."""
    csv_bytes = _make_csv(
        [
            {
                "phone_number": "5511999990001",
                "owner_name": "Alice",
                "address": "Rua A 1",
                "status": "active",
            },
            {
                "phone_number": "5511999990002",
                "owner_name": "Bob",
                "address": "Rua B 2",
                "status": "active",
            },
            {
                "phone_number": "5511999990003",
                "owner_name": "Carol",
                "address": "Rua C 3",
                "status": "active",
            },
        ]
    )

    with (
        patch("app.services.storage.upload_file", return_value="fake-key"),
        patch("app.tasks.processing.process_batch_task.delay"),
    ):
        response = await client.post(
            "/batches",
            files={"file": ("test.csv", io.BytesIO(csv_bytes), "text/csv")},
        )
    batch_id = uuid.UUID(response.json()["id"])

    with patch("app.services.storage.download_file", return_value=csv_bytes):
        await process_batch(db_session, batch_id)

    records_result = await db_session.execute(
        select(BatchRecord).where(BatchRecord.batch_id == batch_id)
    )
    records = records_result.scalars().all()
    assert len(records) == 3
