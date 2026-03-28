from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db, require_role
from app.models.tenant import TenantUser, UserRole
from app.services.erasure import erase_data_for_phone

router = APIRouter()


@router.post("/phone/{phone_number}", response_model=dict)
async def erasure_by_phone(
    phone_number: str,
    current_user: Annotated[TenantUser, Depends(require_role(UserRole.ADMIN))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    """
    LGPD erasure: delete all data for a given phone number within the current tenant.

    Admin role is required.  Returns the counts of deleted rows per entity type.
    """
    return await erase_data_for_phone(
        db=db,
        tenant_id=current_user.tenant_id,
        phone_number=phone_number,
    )
