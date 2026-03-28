import uuid
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.batch import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    BatchValidationError,
)
from app.services import (
    column_mapper,
    dedup,
    entity_validator,
    file_parser,
    storage,
    usage,
)

logger = logging.getLogger(__name__)


async def process_batch(db: AsyncSession, batch_id: uuid.UUID) -> None:
    """
    Full processing pipeline for an uploaded batch.

    Pipeline steps:
    1. Transition UPLOADED -> QUEUED -> PROCESSING
    2. Download file from S3
    3. Parse file (CSV or Excel)
    4. Validate required columns
    5. Create BatchRecord entries in chunks, updating batch.processed_records
    6. Run entity type validation, create BatchValidationError entries
    7. Run deduplication
    8. Auto-map columns (stored in batch.settings)
    9. Transition batch -> REVIEW
    """
    result = await db.execute(select(Batch).where(Batch.id == batch_id))
    batch: Batch | None = result.scalar_one_or_none()
    if batch is None:
        logger.error("Batch %s not found", batch_id)
        return

    # Transition to QUEUED then PROCESSING
    batch.status = BatchStatus.QUEUED
    await db.flush()
    batch.status = BatchStatus.PROCESSING
    await db.flush()

    try:
        # Download file from S3
        file_content = storage.download_file(batch.file_key)

        # Parse file
        headers, rows = file_parser.parse_file(batch.file_name, file_content)

        # Validate required columns
        file_parser.validate_required_columns(headers)

        # Update total records count
        batch.total_records = len(rows)
        await db.flush()

        # Determine entity_type field name in the row (may be "type" or "entity_type")
        entity_type_field = "entity_type" if "entity_type" in headers else "type"

        # Create BatchRecord entries in chunks
        all_records: list[BatchRecord] = []
        for chunk_start in range(0, len(rows), settings.chunk_size):
            chunk = rows[chunk_start : chunk_start + settings.chunk_size]
            chunk_records: list[BatchRecord] = []

            for relative_idx, row in enumerate(chunk):
                row_number = chunk_start + relative_idx + 1
                phone_number = str(row.get("phone_number", "")).strip()
                owner_name = str(row.get("owner_name", "")).strip()
                entity_type = (
                    str(row.get(entity_type_field, "property")).strip() or "property"
                )

                record = BatchRecord(
                    batch_id=batch.id,
                    tenant_id=batch.tenant_id,
                    row_number=row_number,
                    phone_number=phone_number,
                    owner_name=owner_name,
                    entity_type=entity_type,
                    original_data=row,
                    status=BatchRecordStatus.PENDING,
                )
                db.add(record)
                chunk_records.append(record)

            await db.flush()
            all_records.extend(chunk_records)
            batch.processed_records = len(all_records)
            await db.flush()

        # Entity type validation
        validation_errors = await entity_validator.validate_entity_types(
            db, batch.tenant_id, rows, headers
        )
        for err in validation_errors:
            db.add(
                BatchValidationError(
                    batch_id=batch.id,
                    tenant_id=batch.tenant_id,
                    row_number=err["row_number"],
                    error_type=err["error_type"],
                    message=err["message"],
                )
            )
        if validation_errors:
            await db.flush()

        # Deduplication
        duplicate_groups = dedup.detect_duplicates(all_records)
        if duplicate_groups:
            await dedup.apply_dedup_flags(db, duplicate_groups)

        # Transition non-duplicate PENDING records to READY so they are
        # eligible for conversation creation upon batch approval (AC5.2).
        dedup_record_ids: set[uuid.UUID] = set()
        for recs in duplicate_groups.values():
            for r in recs:
                dedup_record_ids.add(r.id)

        for record in all_records:
            if (
                record.id not in dedup_record_ids
                and record.status == BatchRecordStatus.PENDING
            ):
                record.status = BatchRecordStatus.READY
        await db.flush()

        # Auto-map columns (use "property" as default entity_type for mapping)
        default_entity_type = "property"
        mapped, unmapped = await column_mapper.auto_map_columns(
            db, batch.tenant_id, default_entity_type, headers
        )
        batch.settings = {
            **batch.settings,
            "column_mapping": mapped,
            "unmapped_columns": unmapped,
        }
        await db.flush()

        # Record usage for processed records
        await usage.record_event(
            db, batch.tenant_id, "record_processed", count=len(all_records)
        )

        # Transition to REVIEW
        batch.status = BatchStatus.REVIEW
        await db.commit()

    except Exception as exc:
        logger.exception("Batch %s failed during processing: %s", batch_id, exc)
        await db.rollback()
        # Re-fetch after rollback to update status to FAILED
        result2 = await db.execute(select(Batch).where(Batch.id == batch_id))
        batch_retry: Batch | None = result2.scalar_one_or_none()
        if batch_retry is not None:
            batch_retry.status = BatchStatus.FAILED
            await db.commit()
        raise
