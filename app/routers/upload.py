import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.dependencies import get_current_user, get_db
from app.models.batch import Batch, BatchStatus
from app.models.tenant import TenantUser
from app.schemas.batch import BatchResponse
from app.services import file_parser, storage

router = APIRouter()

_ALLOWED_CONTENT_TYPES = {
    "text/csv",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",  # some clients send this for .xlsx
}
_ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
_MAX_FILE_SIZE_BYTES = settings.max_file_size_mb * 1024 * 1024


@router.post("", response_model=BatchResponse, status_code=status.HTTP_201_CREATED)
async def upload_batch(
    file: UploadFile,
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> BatchResponse:
    """
    Upload a CSV or XLSX file to create a new batch.

    - Validates file size (<= 50 MB)
    - Validates file extension (.csv or .xlsx)
    - Streams content to S3/MinIO
    - Creates a Batch entity with UPLOADED status
    - Queues the processing Celery task
    """
    # Validate file extension
    filename = file.filename or ""
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported file type '{ext}'. Only .csv and .xlsx are accepted.",
        )

    # Read content and validate size
    content = await file.read()
    if len(content) > _MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds maximum size of {settings.max_file_size_mb} MB.",
        )

    # Validate required columns synchronously before creating the batch
    try:
        headers, _ = file_parser.parse_file(filename, content)
        file_parser.validate_required_columns(headers)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    # Upload to S3
    file_key = f"{current_user.tenant_id}/{uuid.uuid4()}{ext}"
    storage.upload_file(content, file_key)

    # Create Batch record
    batch = Batch(
        tenant_id=current_user.tenant_id,
        file_name=filename,
        file_key=file_key,
        file_size=len(content),
        status=BatchStatus.UPLOADED,
        total_records=0,
        processed_records=0,
    )
    db.add(batch)
    await db.commit()
    await db.refresh(batch)

    # Queue Celery processing task (import here to avoid circular imports)
    from app.tasks.processing import process_batch_task  # noqa: PLC0415

    process_batch_task.delay(str(batch.id))

    return BatchResponse.model_validate(batch)
