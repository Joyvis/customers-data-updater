from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_db
from app.models.tenant import TenantUser
from app.schemas.usage import UsageResponse
from app.services.usage import get_usage_summary

router = APIRouter()


@router.get("/", response_model=list[UsageResponse])
async def usage_summary(
    current_user: Annotated[TenantUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    period: Annotated[
        str | None,
        Query(
            pattern=r"^\d{4}-\d{2}$",
            description="Filter by period in YYYY-MM format",
        ),
    ] = None,
) -> list[UsageResponse]:
    """Return usage summary for the current tenant, optionally filtered by period."""
    rows = await get_usage_summary(
        db=db,
        tenant_id=current_user.tenant_id,
        period=period,
    )
    return [UsageResponse(**row) for row in rows]
