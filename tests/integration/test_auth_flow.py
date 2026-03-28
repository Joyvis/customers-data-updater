"""
Integration tests: IT3 — Auth flow (login → protected endpoint → token refresh).

Acceptance criteria covered:
  AC-IT3.1: POST /auth/login with valid credentials returns access_token and refresh_token
  AC-IT3.2: A valid access_token grants access to protected endpoints (not 401)
  AC-IT3.3: POST /auth/refresh with a valid refresh_token issues a new access_token
  AC-IT3.4: The refreshed access_token can also access protected endpoints
  AC-IT3.5: POST /auth/login with wrong password returns 401
  AC-IT3.6: Accessing a protected endpoint without a token returns 401 or 403
  AC-IT3.7: POST /auth/refresh with an access_token (not a refresh token) returns 401
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.main import create_app
from app.models import TenantUser

# Reuse the shared test engine / session factory from the top-level conftest.
# We deliberately do NOT use the `client` fixture here — it bypasses real auth.


@pytest.fixture
async def raw_client(db_session: AsyncSession):
    """An AsyncClient with NO dependency overrides — auth goes through the real JWT path."""
    app = create_app()

    async def override_get_db():
        yield db_session

    # Only override DB; leave get_current_user wired to the real JWT verification.
    app.dependency_overrides[get_db] = override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# IT3.1 — Valid login returns tokens
# ---------------------------------------------------------------------------


async def test_it3_login_with_valid_credentials_returns_tokens(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.1: POST /auth/login with correct credentials returns access_token + refresh_token."""
    response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "testpassword123"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert len(data["access_token"]) > 10
    assert len(data["refresh_token"]) > 10


# ---------------------------------------------------------------------------
# IT3.2 — Access token can reach a protected endpoint
# ---------------------------------------------------------------------------


async def test_it3_access_token_allows_access_to_protected_endpoint(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.2: Using the access_token, GET /batches/{nonexistent_id} returns 404 (not 401)."""
    login_response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "testpassword123"},
    )
    access_token = login_response.json()["access_token"]

    nonexistent_id = uuid.uuid4()
    response = await raw_client.get(
        f"/batches/{nonexistent_id}",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    # 404 proves auth passed (we're past the auth layer); 401 would mean auth failed.
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# IT3.3 — Refresh token issues a new access token
# ---------------------------------------------------------------------------


async def test_it3_refresh_token_returns_new_access_token(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.3: POST /auth/refresh with a valid refresh_token returns a new access_token."""
    login_response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "testpassword123"},
    )
    refresh_token = login_response.json()["refresh_token"]

    refresh_response = await raw_client.post(
        "/auth/refresh",
        json={"refresh_token": refresh_token},
    )

    assert refresh_response.status_code == 200, refresh_response.text
    new_data = refresh_response.json()
    assert "access_token" in new_data
    assert "refresh_token" in new_data
    # The new token should be a non-empty JWT string
    assert len(new_data["access_token"]) > 10


# ---------------------------------------------------------------------------
# IT3.4 — Refreshed access token can access protected endpoints
# ---------------------------------------------------------------------------


async def test_it3_refreshed_access_token_allows_access(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.4: The new access_token obtained via refresh can access protected endpoints."""
    login_response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "testpassword123"},
    )
    refresh_token = login_response.json()["refresh_token"]

    refresh_response = await raw_client.post(
        "/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    new_access_token = refresh_response.json()["access_token"]

    nonexistent_id = uuid.uuid4()
    response = await raw_client.get(
        f"/batches/{nonexistent_id}",
        headers={"Authorization": f"Bearer {new_access_token}"},
    )
    assert response.status_code == 404  # auth passed, batch simply doesn't exist


# ---------------------------------------------------------------------------
# IT3.5 — Wrong password returns 401
# ---------------------------------------------------------------------------


async def test_it3_login_with_wrong_password_returns_401(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.5: POST /auth/login with wrong password returns 401."""
    response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "wrongpassword"},
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# IT3.6 — No token → 401 or 403
# ---------------------------------------------------------------------------


async def test_it3_unauthenticated_request_is_rejected(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.6: A request without a Bearer token is rejected with 401 or 403."""
    response = await raw_client.get(f"/batches/{uuid.uuid4()}")
    assert response.status_code in (401, 403)


# ---------------------------------------------------------------------------
# IT3.7 — Using access_token as refresh_token returns 401
# ---------------------------------------------------------------------------


async def test_it3_using_access_token_as_refresh_token_returns_401(
    raw_client: AsyncClient,
    admin_user: TenantUser,
) -> None:
    """AC-IT3.7: POSTing an access_token to /auth/refresh returns 401."""
    login_response = await raw_client.post(
        "/auth/login",
        json={"email": "admin@test-agency.com", "password": "testpassword123"},
    )
    access_token = login_response.json()["access_token"]

    response = await raw_client.post(
        "/auth/refresh",
        json={"refresh_token": access_token},  # deliberately wrong token type
    )
    assert response.status_code == 401
