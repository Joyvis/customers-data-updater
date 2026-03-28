from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_db
from app.models.mapping import ColumnMapping
from app.models.tenant import TenantUser
from app.schemas.mapping import ColumnMappingCreate, ColumnMappingResponse

router = APIRouter()


@router.get("", response_model=list[ColumnMappingResponse])
async def list_column_mappings(
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[ColumnMappingResponse]:
    """List all column mappings for the current tenant."""
    result = await db.execute(
        select(ColumnMapping).where(ColumnMapping.tenant_id == current_user.tenant_id)
    )
    mappings = list(result.scalars().all())
    return [ColumnMappingResponse.model_validate(m) for m in mappings]


@router.put("", response_model=ColumnMappingResponse)
async def upsert_column_mapping(
    payload: ColumnMappingCreate,
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ColumnMappingResponse:
    """
    Create or update a column mapping for the current tenant.

    If a mapping with the same tenant_id + entity_type + original_name already
    exists, it is updated in place. Otherwise a new one is created.
    """
    result = await db.execute(
        select(ColumnMapping).where(
            ColumnMapping.tenant_id == current_user.tenant_id,
            ColumnMapping.entity_type == payload.entity_type,
            ColumnMapping.original_name == payload.original_name,
        )
    )
    existing: ColumnMapping | None = result.scalar_one_or_none()

    if existing is not None:
        existing.friendly_name = payload.friendly_name
        await db.commit()
        await db.refresh(existing)
        return ColumnMappingResponse.model_validate(existing)

    new_mapping = ColumnMapping(
        tenant_id=current_user.tenant_id,
        entity_type=payload.entity_type,
        original_name=payload.original_name,
        friendly_name=payload.friendly_name,
    )
    db.add(new_mapping)
    await db.commit()
    await db.refresh(new_mapping)
    return ColumnMappingResponse.model_validate(new_mapping)
