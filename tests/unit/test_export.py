"""
Unit tests for the CSV/Excel export service.

T26: AC-Export-1 — Download CSV has updated values, friendly names, status column
"""

import csv
import io

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    ColumnMapping,
    Tenant,
)
from app.services.export import generate_export


# ---------------------------------------------------------------------------
# T26: CSV export uses updated values, friendly names, and includes status column
# AC-Export-1: generate_export must produce a CSV whose headers are friendly names,
#              whose data cells reflect updated_data over original_data, and which
#              includes a "status" column with the outcome label.
# ---------------------------------------------------------------------------


async def test_t26_csv_export_has_friendly_names_updated_values_and_status(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T26 — AC-Export-1: CSV export uses friendly column names, updated values, status column."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="export.csv",
        file_key="test/export.csv",
        file_size=2048,
        status=BatchStatus.COMPLETED,
    )
    db_session.add(batch)
    await db_session.flush()

    # Column mappings: original → friendly
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
            friendly_name="Nome do Proprietário",
        )
    )
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="price",
            friendly_name="Preço (R$)",
        )
    )

    # Record 1: confirmed, price updated from 400000 → 450000
    record_1 = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990100",
        owner_name="Helena Braga",
        entity_type="property",
        original_data={
            "phone_number": "5511999990100",
            "owner_name": "Helena Braga",
            "price": "400000",
        },
        updated_data={
            "phone_number": "5511999990100",
            "owner_name": "Helena Braga",
            "price": "450000",
        },
        status=BatchRecordStatus.COMPLETED,
    )
    # Record 2: dead letter, no updated data
    record_2 = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=2,
        phone_number="5511999990101",
        owner_name="Roberto Dias",
        entity_type="property",
        original_data={
            "phone_number": "5511999990101",
            "owner_name": "Roberto Dias",
            "price": "300000",
        },
        updated_data=None,
        status=BatchRecordStatus.DEAD_LETTER,
    )
    db_session.add(record_1)
    db_session.add(record_2)
    await db_session.commit()

    csv_bytes = await generate_export(
        db=db_session,
        batch_id=batch.id,
        tenant_id=tenant.id,
        format="csv",
    )

    assert isinstance(csv_bytes, bytes)

    # Decode the BOM-aware UTF-8 CSV
    content = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    # Verify headers are friendly names
    assert reader.fieldnames is not None
    assert "Telefone" in reader.fieldnames
    assert "Nome do Proprietário" in reader.fieldnames
    assert "Preço (R$)" in reader.fieldnames
    assert "status" in reader.fieldnames

    # Original technical column names must NOT appear as headers
    assert "phone_number" not in reader.fieldnames
    assert "owner_name" not in reader.fieldnames
    assert "price" not in reader.fieldnames

    assert len(rows) == 2

    # Row 1: updated price should be reflected
    row1 = next(r for r in rows if r["Telefone"] == "5511999990100")
    assert row1["Preço (R$)"] == "450000"
    assert row1["status"] == "confirmed"

    # Row 2: original price (no update), dead_letter status
    row2 = next(r for r in rows if r["Telefone"] == "5511999990101")
    assert row2["Preço (R$)"] == "300000"
    assert row2["status"] == "dead_letter"


async def test_t26_csv_export_falls_back_to_original_when_no_updated_data(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T26 edge case — Records without updated_data use original_data values."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="noUpdate.csv",
        file_key="test/noUpdate.csv",
        file_size=512,
        status=BatchStatus.COMPLETED,
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

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990110",
        owner_name="Sonia Barros",
        entity_type="property",
        original_data={"phone_number": "5511999990110"},
        updated_data=None,
        status=BatchRecordStatus.COMPLETED,
    )
    db_session.add(record)
    await db_session.commit()

    csv_bytes = await generate_export(db_session, batch.id, tenant.id, format="csv")
    content = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["Telefone"] == "5511999990110"


async def test_t26_csv_export_uses_raw_column_name_when_no_mapping(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T26 edge case — Columns without a ColumnMapping entry fall back to the raw name."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="nomap.csv",
        file_key="test/nomap.csv",
        file_size=512,
        status=BatchStatus.COMPLETED,
    )
    db_session.add(batch)
    await db_session.flush()

    # No ColumnMapping entries added — raw column names must appear
    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990120",
        owner_name="Tomás Freitas",
        entity_type="property",
        original_data={"phone_number": "5511999990120", "some_col": "value"},
        updated_data=None,
        status=BatchRecordStatus.SKIPPED,
    )
    db_session.add(record)
    await db_session.commit()

    csv_bytes = await generate_export(db_session, batch.id, tenant.id, format="csv")
    content = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))

    assert "phone_number" in reader.fieldnames
    assert "some_col" in reader.fieldnames
    assert "status" in reader.fieldnames
