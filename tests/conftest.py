from typing import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.dependencies import get_current_user, get_db
from app.main import create_app
from app.models import EntityTypeConfig, Tenant, TenantUser, UserRole
from app.services.auth import hash_password

TEST_DATABASE_URL = "sqlite+aiosqlite:///./test.db"

engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestSessionFactory = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


@pytest.fixture(autouse=True)
async def setup_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    async with TestSessionFactory() as session:
        yield session


@pytest.fixture
async def tenant(db_session: AsyncSession) -> Tenant:
    t = Tenant(name="Test Agency", slug="test-agency", settings={})
    db_session.add(t)
    await db_session.flush()

    config = EntityTypeConfig(
        tenant_id=t.id,
        entity_type="property",
        required_columns=["address", "status"],
    )
    db_session.add(config)
    await db_session.commit()
    await db_session.refresh(t)
    return t


@pytest.fixture
async def admin_user(db_session: AsyncSession, tenant: Tenant) -> TenantUser:
    user = TenantUser(
        tenant_id=tenant.id,
        email="admin@test-agency.com",
        hashed_password=hash_password("testpassword123"),
        full_name="Admin User",
        role=UserRole.ADMIN,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


@pytest.fixture
async def operator_user(db_session: AsyncSession, tenant: Tenant) -> TenantUser:
    user = TenantUser(
        tenant_id=tenant.id,
        email="operator@test-agency.com",
        hashed_password=hash_password("testpassword123"),
        full_name="Operator User",
        role=UserRole.OPERATOR,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


@pytest.fixture
async def tenant_b(db_session: AsyncSession) -> Tenant:
    t = Tenant(name="Other Agency", slug="other-agency", settings={})
    db_session.add(t)
    await db_session.flush()
    config = EntityTypeConfig(
        tenant_id=t.id,
        entity_type="property",
        required_columns=["address", "status"],
    )
    db_session.add(config)
    await db_session.commit()
    await db_session.refresh(t)
    return t


@pytest.fixture
async def client(db_session: AsyncSession, admin_user: TenantUser):
    app = create_app()

    async def override_get_db():
        yield db_session

    async def override_get_current_user():
        return admin_user

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
