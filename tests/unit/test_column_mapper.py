"""
Unit tests for app/services/column_mapper.py

Coverage:
  T11 - AC: auto_map_columns flags all non-standard columns as unmapped when no DB mappings exist
  T12 - AC: auto_map_columns uses a saved ColumnMapping to auto-map a previously unknown column
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ColumnMapping
from app.services.column_mapper import auto_map_columns


# ---------------------------------------------------------------------------
# T11: No existing mappings → all non-standard columns flagged as unmapped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_map_columns_no_mappings_standard_cols_mapped(
    db_session: AsyncSession, tenant
):
    """
    T11 — With no saved ColumnMappings, standard columns (phone_number, owner_name)
    are auto-recognized and 'endereco', 'valor' are reported as unmapped.
    """
    headers = ["phone_number", "owner_name", "endereco", "valor"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    # Standard columns are always mapped to themselves
    assert "phone_number" in mapped
    assert mapped["phone_number"] == "phone_number"
    assert "owner_name" in mapped
    assert mapped["owner_name"] == "owner_name"

    # Non-standard, unmapped columns must be in unmapped list
    assert "endereco" in unmapped
    assert "valor" in unmapped

    # Non-standard columns must NOT appear in mapped
    assert "endereco" not in mapped
    assert "valor" not in mapped


@pytest.mark.asyncio
async def test_auto_map_columns_no_mappings_returns_correct_counts(
    db_session: AsyncSession, tenant
):
    """T11 (count) — mapped has 2 standard entries, unmapped has 2 custom entries."""
    headers = ["phone_number", "owner_name", "endereco", "valor"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert len(mapped) == 2
    assert len(unmapped) == 2


@pytest.mark.asyncio
async def test_auto_map_columns_type_column_is_standard(
    db_session: AsyncSession, tenant
):
    """T11 (type col) — 'type' is a standard column and is always auto-mapped."""
    headers = ["phone_number", "owner_name", "type"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "type" in mapped
    assert mapped["type"] == "type"
    assert unmapped == []


@pytest.mark.asyncio
async def test_auto_map_columns_all_standard_headers_no_unmapped(
    db_session: AsyncSession, tenant
):
    """T11 (edge) — Headers that are all standard produce empty unmapped list."""
    headers = ["phone_number", "owner_name", "type"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert unmapped == []
    assert len(mapped) == 3


# ---------------------------------------------------------------------------
# T12: Existing mapping for "endereco" → auto-mapped on subsequent upload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_map_columns_uses_saved_mapping_for_custom_column(
    db_session: AsyncSession, tenant
):
    """
    T12 — After saving a ColumnMapping for 'endereco', auto_map_columns recognises
    it and includes it in the mapped dict with the saved friendly_name.
    """
    # Persist a ColumnMapping for "endereco"
    mapping = ColumnMapping(
        tenant_id=tenant.id,
        entity_type="property",
        original_name="endereco",
        friendly_name="Endereço",
    )
    db_session.add(mapping)
    await db_session.commit()

    headers = ["phone_number", "owner_name", "endereco", "valor"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    # "endereco" is now recognised via the saved mapping
    assert "endereco" in mapped
    assert mapped["endereco"] == "Endereço"

    # "valor" still has no mapping
    assert "valor" in unmapped
    assert "valor" not in mapped


@pytest.mark.asyncio
async def test_auto_map_columns_mapping_lookup_is_case_insensitive(
    db_session: AsyncSession, tenant
):
    """T12 (case) — Mapping lookup is case-insensitive for both stored and file header names."""
    mapping = ColumnMapping(
        tenant_id=tenant.id,
        entity_type="property",
        original_name="Endereco",  # stored with capital E
        friendly_name="Endereço",
    )
    db_session.add(mapping)
    await db_session.commit()

    # File header uses all-lowercase
    headers = ["phone_number", "owner_name", "endereco"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "endereco" in mapped
    assert mapped["endereco"] == "Endereço"
    assert unmapped == []


@pytest.mark.asyncio
async def test_auto_map_columns_mapping_isolation_per_tenant(
    db_session: AsyncSession, tenant, tenant_b
):
    """T12 (isolation) — A mapping saved for tenant_b is NOT applied for tenant."""
    mapping = ColumnMapping(
        tenant_id=tenant_b.id,
        entity_type="property",
        original_name="endereco",
        friendly_name="Endereço",
    )
    db_session.add(mapping)
    await db_session.commit()

    headers = ["phone_number", "owner_name", "endereco"]

    # Lookup for tenant (not tenant_b) — should not find the mapping
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "endereco" in unmapped
    assert "endereco" not in mapped


@pytest.mark.asyncio
async def test_auto_map_columns_mapping_isolation_per_entity_type(
    db_session: AsyncSession, tenant
):
    """T12 (entity isolation) — Mappings for a different entity_type are not applied."""
    mapping = ColumnMapping(
        tenant_id=tenant.id,
        entity_type="land",  # different entity type
        original_name="endereco",
        friendly_name="Endereço",
    )
    db_session.add(mapping)
    await db_session.commit()

    headers = ["phone_number", "owner_name", "endereco"]

    # Looking up for "property", not "land"
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "endereco" in unmapped
    assert "endereco" not in mapped
