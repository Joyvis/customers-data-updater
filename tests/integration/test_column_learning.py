"""
Integration tests: IT6 — Column-mapping learning across batches.

Acceptance criteria covered:
  AC-IT6.1: After processing a batch with unknown columns, those columns appear in unmapped list
  AC-IT6.2: After adding ColumnMapping entries for previously unmapped columns, a second batch
             with the same columns auto-maps them (they no longer appear as unmapped)
  AC-IT6.3: A brand-new column in the second batch that was never mapped still appears unmapped
  AC-IT6.4: Standard columns (phone_number, owner_name) are always auto-mapped; never unmapped
"""

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ColumnMapping, Tenant
from app.services.column_mapper import auto_map_columns


# ---------------------------------------------------------------------------
# IT6.1 — Unknown columns are returned as unmapped by auto_map_columns
# ---------------------------------------------------------------------------


async def test_it6_unknown_columns_appear_in_unmapped_list(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT6.1: Columns with no stored ColumnMapping are returned in the unmapped list."""
    headers = ["phone_number", "owner_name", "endereco", "valor"]

    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    # Standard columns are always mapped
    assert "phone_number" in mapped
    assert "owner_name" in mapped

    # Custom columns without a ColumnMapping entry should be unmapped
    assert "endereco" in unmapped
    assert "valor" in unmapped


# ---------------------------------------------------------------------------
# IT6.2 — After adding mappings, the same columns are auto-mapped in a subsequent call
# ---------------------------------------------------------------------------


async def test_it6_learned_columns_are_auto_mapped_in_second_batch(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT6.2: Columns that were manually mapped appear in `mapped` for later batches."""
    # Simulate operator manually creating ColumnMappings after the first batch
    for original, friendly in [("endereco", "Endereço"), ("valor", "Valor do Imóvel")]:
        db_session.add(
            ColumnMapping(
                tenant_id=tenant.id,
                entity_type="property",
                original_name=original,
                friendly_name=friendly,
            )
        )
    await db_session.commit()

    headers = ["phone_number", "owner_name", "endereco", "valor"]
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "endereco" in mapped
    assert mapped["endereco"] == "Endereço"
    assert "valor" in mapped
    assert mapped["valor"] == "Valor do Imóvel"
    assert "endereco" not in unmapped
    assert "valor" not in unmapped


# ---------------------------------------------------------------------------
# IT6.3 — A brand-new column in the second batch is still unmapped
# ---------------------------------------------------------------------------


async def test_it6_new_column_in_second_batch_remains_unmapped(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT6.3: A column that was never mapped continues to appear in unmapped after learning."""
    # Add mappings for the previously-known columns
    for original, friendly in [("endereco", "Endereço"), ("valor", "Valor do Imóvel")]:
        db_session.add(
            ColumnMapping(
                tenant_id=tenant.id,
                entity_type="property",
                original_name=original,
                friendly_name=friendly,
            )
        )
    await db_session.commit()

    # Second batch introduces "novo_campo" which has never been mapped
    headers = ["phone_number", "owner_name", "endereco", "valor", "novo_campo"]
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "novo_campo" in unmapped
    # The previously learned columns must still be mapped
    assert "endereco" in mapped
    assert "valor" in mapped


# ---------------------------------------------------------------------------
# IT6.4 — Standard columns are always auto-mapped
# ---------------------------------------------------------------------------


async def test_it6_standard_columns_always_auto_mapped(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT6.4: phone_number and owner_name are always in mapped, regardless of stored entries."""
    headers = ["phone_number", "owner_name"]
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "phone_number" in mapped
    assert mapped["phone_number"] == "phone_number"
    assert "owner_name" in mapped
    assert mapped["owner_name"] == "owner_name"
    assert "phone_number" not in unmapped
    assert "owner_name" not in unmapped


# ---------------------------------------------------------------------------
# IT6 edge — Mapping lookup is case-insensitive for original_name
# ---------------------------------------------------------------------------


async def test_it6_mapping_lookup_is_case_insensitive(
    db_session: AsyncSession,
    tenant: Tenant,
) -> None:
    """AC-IT6.2 (edge): Stored original_name matching is case-insensitive."""
    db_session.add(
        ColumnMapping(
            tenant_id=tenant.id,
            entity_type="property",
            original_name="Endereco",  # stored with capital E
            friendly_name="Endereço",
        )
    )
    await db_session.commit()

    # File header uses lowercase
    headers = ["phone_number", "owner_name", "endereco"]
    mapped, unmapped = await auto_map_columns(
        db_session, tenant.id, "property", headers
    )

    assert "endereco" in mapped
    assert "endereco" not in unmapped
