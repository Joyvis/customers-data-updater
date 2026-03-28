from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.models.mapping import EntityTypeConfig
from app.models.tenant import Tenant, TenantUser, UserRole
from app.schemas.tenant import TenantCreate, TenantResponse
from app.services.auth import hash_password

router = APIRouter()


@router.post("/", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    body: TenantCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TenantResponse:
    result = await db.execute(select(Tenant).where(Tenant.slug == body.slug))
    existing = result.scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A tenant with this slug already exists",
        )

    tenant = Tenant(name=body.name, slug=body.slug)
    db.add(tenant)
    await db.flush()

    admin_user = TenantUser(
        tenant_id=tenant.id,
        email=body.admin_email,
        hashed_password=hash_password(body.admin_password),
        full_name=body.admin_name,
        role=UserRole.ADMIN,
    )
    db.add(admin_user)

    default_config = EntityTypeConfig(
        tenant_id=tenant.id,
        entity_type="property",
        required_columns=["address", "status"],
        settings={},
    )
    db.add(default_config)

    await db.commit()
    await db.refresh(tenant)

    return TenantResponse.model_validate(tenant)
