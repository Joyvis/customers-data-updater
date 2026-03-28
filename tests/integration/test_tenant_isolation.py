"""
Integration tests: IT2 — Tenant isolation.

Acceptance criteria covered:
  AC-IT2.1: A batch belonging to tenant A is not visible to tenant B (GET returns 404)
  AC-IT2.2: The same batch is visible to tenant A (GET returns 200)
  AC-IT2.3: BatchRecords created for tenant A are not accessible by tenant B
"""

import uuid

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_db
from app.main import create_app
from app.models import (
    Batch,
    BatchStatus,
    Tenant,
    TenantUser,
    UserRole,
)
from app.services.auth import hash_password


# ---------------------------------------------------------------------------
# Helper: build a second httpx client that impersonates a tenant_b user
# ---------------------------------------------------------------------------


async def _make_tenant_b_client(db_session: AsyncSession, tenant_b: Tenant):
    """Return an AsyncClient whose auth dependency resolves to a fresh tenant_b user."""
    user_b = TenantUser(
        tenant_id=tenant_b.id,
        email="admin@other-agency.com",
        hashed_password=hash_password("password123"),
        full_name="Tenant B Admin",
        role=UserRole.ADMIN,
    )
    db_session.add(user_b)
    await db_session.commit()
    await db_session.refresh(user_b)

    app = create_app()

    async def override_get_db():
        yield db_session

    async def override_get_current_user():
        return user_b

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


# ---------------------------------------------------------------------------
# IT2.1 — tenant B cannot access tenant A's batch
# ---------------------------------------------------------------------------


async def test_it2_tenant_b_cannot_access_tenant_a_batch(
    db_session: AsyncSession,
    tenant: Tenant,
    tenant_b: Tenant,
    client: AsyncClient,
) -> None:
    """AC-IT2.1: GET /batches/{id} for a batch owned by tenant A returns 404 for tenant B."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="data.csv",
        file_key=f"{tenant.id}/{uuid.uuid4()}.csv",
        file_size=1024,
        status=BatchStatus.REVIEW,
        total_records=3,
        processed_records=3,
    )
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    async with await _make_tenant_b_client(db_session, tenant_b) as tenant_b_client:
        response = await tenant_b_client.get(f"/batches/{batch.id}")

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# IT2.2 — tenant A can access its own batch
# ---------------------------------------------------------------------------


async def test_it2_tenant_a_can_access_own_batch(
    db_session: AsyncSession,
    tenant: Tenant,
    client: AsyncClient,
) -> None:
    """AC-IT2.2: GET /batches/{id} for a batch owned by tenant A returns 200 for tenant A."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="data.csv",
        file_key=f"{tenant.id}/{uuid.uuid4()}.csv",
        file_size=1024,
        status=BatchStatus.REVIEW,
        total_records=3,
        processed_records=3,
    )
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    response = await client.get(f"/batches/{batch.id}")

    assert response.status_code == 200
    assert response.json()["id"] == str(batch.id)


# ---------------------------------------------------------------------------
# IT2.3 — tenant B cannot access batch errors from tenant A
# ---------------------------------------------------------------------------


async def test_it2_tenant_b_cannot_access_tenant_a_batch_errors(
    db_session: AsyncSession,
    tenant: Tenant,
    tenant_b: Tenant,
) -> None:
    """AC-IT2.3: GET /batches/{id}/errors for tenant A's batch returns 404 for tenant B."""
    batch = Batch(
        tenant_id=tenant.id,
        file_name="data.csv",
        file_key=f"{tenant.id}/{uuid.uuid4()}.csv",
        file_size=512,
        status=BatchStatus.REVIEW,
        total_records=1,
        processed_records=1,
    )
    db_session.add(batch)
    await db_session.commit()
    await db_session.refresh(batch)

    async with await _make_tenant_b_client(db_session, tenant_b) as tenant_b_client:
        response = await tenant_b_client.get(f"/batches/{batch.id}/errors")

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# IT2 edge — non-existent batch returns 404 (not 403), guarding against info leak
# ---------------------------------------------------------------------------


async def test_it2_nonexistent_batch_returns_404_not_403(
    client: AsyncClient,
) -> None:
    """AC-IT2.1 (edge): Requesting a completely unknown batch ID returns 404, not 403."""
    fake_id = uuid.uuid4()
    response = await client.get(f"/batches/{fake_id}")
    assert response.status_code == 404
