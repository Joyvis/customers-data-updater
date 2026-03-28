import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_db
from app.models.batch import Batch
from app.models.tenant import TenantUser
from app.services.export import EXPORT_ALLOWED_STATUSES, generate_export

router = APIRouter()


@router.get("/{batch_id}/download")
async def download_export(
    batch_id: uuid.UUID,
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    format: Annotated[str, Query(pattern="^(csv|xlsx)$")] = "csv",
) -> StreamingResponse:
    """Download an export file for a completed (or partially completed) batch."""
    result = await db.execute(
        select(Batch).where(
            Batch.id == batch_id,
            Batch.tenant_id == current_user.tenant_id,
        )
    )
    batch = result.scalar_one_or_none()

    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found"
        )

    if batch.status not in EXPORT_ALLOWED_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Batch is not ready for export. "
                f"Current status: {batch.status.value}. "
                f"Allowed: {', '.join(s.value for s in EXPORT_ALLOWED_STATUSES)}"
            ),
        )

    file_bytes = await generate_export(
        db=db,
        batch_id=batch_id,
        tenant_id=current_user.tenant_id,
        format=format,
    )

    if format == "xlsx":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = f"batch_{batch_id}.xlsx"
    else:
        media_type = "text/csv; charset=utf-8"
        filename = f"batch_{batch_id}.csv"

    import io

    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
