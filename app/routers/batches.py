import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.dependencies import get_current_user, get_db
from app.models.batch import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    BatchValidationError,
)
from app.models.conversation import Conversation, ConversationStatus
from app.models.mapping import ColumnMapping
from app.models.opt_out import OptOutList
from app.models.tenant import TenantUser
from app.schemas.batch import (
    BatchRecordResponse,
    BatchResponse,
    ValidationErrorResponse,
)
from app.services.file_parser import STANDARD_COLUMNS

router = APIRouter()


# ---------------------------------------------------------------------------
# Inline request/response schemas
# ---------------------------------------------------------------------------


class DedupResolveRequest(BaseModel):
    action: str  # "merge" or "skip"
    primary_record_id: uuid.UUID | None = None


class DedupGroupResponse(BaseModel):
    dedup_group_id: str
    records: list[BatchRecordResponse]


class UnmappedColumnsResponse(BaseModel):
    unmapped_columns: list[str]


class RecentlyRefreshedRecord(BaseModel):
    record: BatchRecordResponse
    last_refreshed_batch_id: uuid.UUID
    last_refreshed_at: datetime


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _get_batch_for_tenant(
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    db: AsyncSession,
) -> Batch:
    result = await db.execute(
        select(Batch).where(
            Batch.id == batch_id,
            Batch.tenant_id == tenant_id,
        )
    )
    batch = result.scalar_one_or_none()
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found"
        )
    return batch


# ---------------------------------------------------------------------------
# GET /{batch_id}
# ---------------------------------------------------------------------------


@router.get("/{batch_id}", response_model=BatchResponse)
async def get_batch(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> BatchResponse:
    batch = await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)
    return BatchResponse.model_validate(batch)


# ---------------------------------------------------------------------------
# GET /{batch_id}/errors
# ---------------------------------------------------------------------------


@router.get("/{batch_id}/errors", response_model=list[ValidationErrorResponse])
async def get_batch_errors(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> list[ValidationErrorResponse]:
    await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    result = await db.execute(
        select(BatchValidationError).where(
            BatchValidationError.batch_id == batch_id,
            BatchValidationError.tenant_id == current_user.tenant_id,
        )
    )
    errors = result.scalars().all()
    return [ValidationErrorResponse.model_validate(e) for e in errors]


# ---------------------------------------------------------------------------
# GET /{batch_id}/dedup-groups
# ---------------------------------------------------------------------------


@router.get("/{batch_id}/dedup-groups", response_model=list[DedupGroupResponse])
async def get_dedup_groups(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> list[DedupGroupResponse]:
    await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
            BatchRecord.dedup_group_id.is_not(None),
        )
    )
    records = result.scalars().all()

    groups: dict[str, list[BatchRecord]] = {}
    for record in records:
        group_key: str = record.dedup_group_id or ""
        groups.setdefault(group_key, []).append(record)

    return [
        DedupGroupResponse(
            dedup_group_id=group_id,
            records=[BatchRecordResponse.model_validate(r) for r in group_records],
        )
        for group_id, group_records in groups.items()
    ]


# ---------------------------------------------------------------------------
# POST /{batch_id}/dedup-groups/{group_id}/resolve
# ---------------------------------------------------------------------------


@router.post(
    "/{batch_id}/dedup-groups/{group_id}/resolve",
    response_model=list[BatchRecordResponse],
)
async def resolve_dedup_group(
    batch_id: uuid.UUID,
    group_id: str,
    body: DedupResolveRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> list[BatchRecordResponse]:
    await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    if body.action not in ("merge", "skip"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="action must be 'merge' or 'skip'",
        )

    result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
            BatchRecord.dedup_group_id == group_id,
        )
    )
    records = result.scalars().all()

    if not records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dedup group not found"
        )

    if body.action == "merge":
        if body.primary_record_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="primary_record_id is required for merge action",
            )
        primary_ids = {r.id for r in records if r.id == body.primary_record_id}
        if not primary_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="primary_record_id does not belong to this dedup group",
            )
        for record in records:
            if record.id == body.primary_record_id:
                record.status = BatchRecordStatus.READY
            else:
                record.status = BatchRecordStatus.SKIPPED

    else:  # skip
        for record in records:
            record.status = BatchRecordStatus.SKIPPED

    await db.commit()
    for record in records:
        await db.refresh(record)

    return [BatchRecordResponse.model_validate(r) for r in records]


# ---------------------------------------------------------------------------
# GET /{batch_id}/unmapped-columns
# ---------------------------------------------------------------------------


@router.get("/{batch_id}/unmapped-columns", response_model=UnmappedColumnsResponse)
async def get_unmapped_columns(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> UnmappedColumnsResponse:
    await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    # Fetch a sample of records to collect original_data keys
    records_result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
        )
    )
    records = records_result.scalars().all()

    # Collect all unique column names across records
    all_columns: set[str] = set()
    entity_types: set[str] = set()
    for record in records:
        if record.original_data:
            all_columns.update(record.original_data.keys())
        entity_types.add(record.entity_type)

    if not all_columns:
        return UnmappedColumnsResponse(unmapped_columns=[])

    # Fetch existing mappings for this tenant across relevant entity_types
    mappings_result = await db.execute(
        select(ColumnMapping).where(
            ColumnMapping.tenant_id == current_user.tenant_id,
            ColumnMapping.entity_type.in_(list(entity_types)),
        )
    )
    mappings = mappings_result.scalars().all()
    mapped_columns = {m.original_name for m in mappings}

    unmapped = sorted(all_columns - mapped_columns - STANDARD_COLUMNS)
    return UnmappedColumnsResponse(unmapped_columns=unmapped)


# ---------------------------------------------------------------------------
# GET /{batch_id}/recently-refreshed
# ---------------------------------------------------------------------------


@router.get(
    "/{batch_id}/recently-refreshed", response_model=list[RecentlyRefreshedRecord]
)
async def get_recently_refreshed(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> list[RecentlyRefreshedRecord]:
    await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    # Records in the current batch
    current_records_result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
        )
    )
    current_records = current_records_result.scalars().all()

    if not current_records:
        return []

    current_phone_numbers = {r.phone_number for r in current_records}

    # Cutoff window
    cutoff = datetime.now(tz=timezone.utc) - timedelta(
        days=settings.recently_refreshed_days
    )

    # Find completed batches within the window (excluding this batch)
    completed_batches_result = await db.execute(
        select(Batch).where(
            Batch.tenant_id == current_user.tenant_id,
            Batch.id != batch_id,
            Batch.status.in_([BatchStatus.COMPLETED, BatchStatus.PARTIALLY_COMPLETED]),
            Batch.updated_at >= cutoff,
        )
    )
    completed_batches = completed_batches_result.scalars().all()

    if not completed_batches:
        return []

    completed_batch_ids = [b.id for b in completed_batches]
    completed_batch_map = {b.id: b for b in completed_batches}

    # Find records in those batches that share phone numbers with the current batch
    other_records_result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id.in_(completed_batch_ids),
            BatchRecord.tenant_id == current_user.tenant_id,
            BatchRecord.phone_number.in_(list(current_phone_numbers)),
        )
    )
    other_records = other_records_result.scalars().all()

    # Build a map of phone_number -> most-recent (batch_id, updated_at) from completed batches
    phone_to_recent: dict[str, tuple[uuid.UUID, datetime]] = {}
    for other in other_records:
        batch_updated_at = completed_batch_map[other.batch_id].updated_at
        if other.phone_number not in phone_to_recent:
            phone_to_recent[other.phone_number] = (other.batch_id, batch_updated_at)
        else:
            existing_dt = phone_to_recent[other.phone_number][1]
            if batch_updated_at > existing_dt:
                phone_to_recent[other.phone_number] = (other.batch_id, batch_updated_at)

    flagged: list[RecentlyRefreshedRecord] = []
    for record in current_records:
        if record.phone_number in phone_to_recent:
            ref_batch_id, ref_at = phone_to_recent[record.phone_number]
            flagged.append(
                RecentlyRefreshedRecord(
                    record=BatchRecordResponse.model_validate(record),
                    last_refreshed_batch_id=ref_batch_id,
                    last_refreshed_at=ref_at,
                )
            )

    return flagged


# ---------------------------------------------------------------------------
# POST /{batch_id}/approve
# ---------------------------------------------------------------------------


@router.post("/{batch_id}/approve", response_model=BatchResponse)
async def approve_batch(
    batch_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[TenantUser, Depends(get_current_user)],
) -> BatchResponse:
    batch = await _get_batch_for_tenant(batch_id, current_user.tenant_id, db)

    # Must be in REVIEW status
    if batch.status != BatchStatus.REVIEW:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Batch must be in REVIEW status to approve; current status: {batch.status}",
        )

    # Block if unresolved dedup groups exist
    dedup_result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
            BatchRecord.status == BatchRecordStatus.DEDUP_REVIEW,
        )
    )
    unresolved_dedup = dedup_result.scalars().all()
    if unresolved_dedup:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Batch has {len(unresolved_dedup)} unresolved dedup group(s). Resolve all dedup groups before approving.",
        )

    # Block if unmapped columns exist — reuse the same logic
    records_result = await db.execute(
        select(BatchRecord).where(
            BatchRecord.batch_id == batch_id,
            BatchRecord.tenant_id == current_user.tenant_id,
        )
    )
    all_records = records_result.scalars().all()

    all_columns: set[str] = set()
    entity_types: set[str] = set()
    for record in all_records:
        if record.original_data:
            all_columns.update(record.original_data.keys())
        entity_types.add(record.entity_type)

    if all_columns:
        mappings_result = await db.execute(
            select(ColumnMapping).where(
                ColumnMapping.tenant_id == current_user.tenant_id,
                ColumnMapping.entity_type.in_(list(entity_types)),
            )
        )
        mappings = mappings_result.scalars().all()
        mapped_columns = {m.original_name for m in mappings}
        unmapped = all_columns - mapped_columns - STANDARD_COLUMNS
        if unmapped:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Batch has unmapped columns that must be resolved before approving: {sorted(unmapped)}",
            )

    # Fetch opted-out phone numbers for this tenant
    opt_out_result = await db.execute(
        select(OptOutList.phone_number).where(
            OptOutList.tenant_id == current_user.tenant_id,
        )
    )
    opted_out_phones: set[str] = {row[0] for row in opt_out_result.all()}

    # Transition batch to APPROVED
    batch.status = BatchStatus.APPROVED

    # Create Conversation entities for eligible records; mark opted-out records
    for record in all_records:
        if record.status != BatchRecordStatus.READY:
            continue
        if record.phone_number in opted_out_phones:
            record.status = BatchRecordStatus.OPTED_OUT
            continue
        conversation = Conversation(
            batch_record_id=record.id,
            tenant_id=current_user.tenant_id,
            phone_number=record.phone_number,
            status=ConversationStatus.READY,
            max_messages=batch.max_messages_per_conversation,
        )
        db.add(conversation)

    await db.commit()
    await db.refresh(batch)

    # Trigger outreach asynchronously after commit
    from app.tasks.outreach import send_initial_outreach_task

    send_initial_outreach_task.delay(str(batch.id))

    return BatchResponse.model_validate(batch)
