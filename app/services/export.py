import csv
import io
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.orm import selectinload

from app.models.batch import BatchRecord, BatchRecordStatus, BatchStatus
from app.models.mapping import ColumnMapping

EXPORT_ALLOWED_STATUSES = {BatchStatus.COMPLETED, BatchStatus.PARTIALLY_COMPLETED}

# Map BatchRecordStatus values to friendly export labels
RECORD_STATUS_LABELS: dict[str, str] = {
    BatchRecordStatus.DEAD_LETTER: "dead_letter",
    BatchRecordStatus.SKIPPED: "skipped",
    BatchRecordStatus.OPTED_OUT: "opted_out",
}


def _get_record_status_label(record: BatchRecord) -> str:
    """Determine the export status label, distinguishing 'confirmed' vs 'updated'."""
    if record.status in RECORD_STATUS_LABELS:
        return RECORD_STATUS_LABELS[record.status]
    if record.status == BatchRecordStatus.COMPLETED:
        # Check conversation classification to distinguish confirmed vs updated
        for conv in record.conversations:
            if conv.classification == "updated":
                return "updated"
        return "confirmed"
    return record.status.value


async def generate_export(
    db: AsyncSession,
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    format: str = "csv",
) -> bytes:
    """
    Generate a CSV or Excel export of all BatchRecords for a completed batch.

    Returns raw bytes ready to be sent as a file download.
    """
    # Load all records for this batch (already scoped to tenant via batch ownership)
    result = await db.execute(
        select(BatchRecord)
        .options(selectinload(BatchRecord.conversations))
        .where(BatchRecord.batch_id == batch_id, BatchRecord.tenant_id == tenant_id)
        .order_by(BatchRecord.row_number)
    )
    records: list[BatchRecord] = list(result.scalars().all())

    # Build column name -> friendly name mapping for this tenant
    # We need to gather all entity types present in the batch
    entity_types = {r.entity_type for r in records}
    friendly_map: dict[str, str] = {}
    for entity_type in entity_types:
        mapping_result = await db.execute(
            select(ColumnMapping).where(
                ColumnMapping.tenant_id == tenant_id,
                ColumnMapping.entity_type == entity_type,
            )
        )
        for cm in mapping_result.scalars().all():
            # Later entity types can overwrite earlier ones; that's acceptable for
            # mixed-type batches since column names should be consistent.
            friendly_map[cm.original_name] = cm.friendly_name

    # Collect all column keys across all records (preserve insertion order)
    all_keys: list[str] = []
    seen_keys: set[str] = set()
    for record in records:
        for key in record.original_data.keys():
            if key not in seen_keys:
                all_keys.append(key)
                seen_keys.add(key)

    # Build friendly header names
    def _friendly(col: str) -> str:
        return friendly_map.get(col, col)

    headers = [_friendly(k) for k in all_keys] + ["status"]

    # Build rows
    rows: list[list] = []
    for record in records:
        row: list = []
        for key in all_keys:
            # Use updated_data value if available, else fall back to original
            if record.updated_data and key in record.updated_data:
                row.append(record.updated_data[key])
            else:
                row.append(record.original_data.get(key, ""))
        status_label = _get_record_status_label(record)
        row.append(status_label)
        rows.append(row)

    if format == "xlsx":
        return _build_excel(headers, rows)
    return _build_csv(headers, rows)


def _build_csv(headers: list[str], rows: list[list]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def _build_excel(headers: list[str], rows: list[list]) -> bytes:
    try:
        import openpyxl
    except ImportError as exc:
        raise RuntimeError("openpyxl is required for Excel export") from exc

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Export"
    ws.append(headers)
    for row in rows:
        ws.append(row)

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.read()
