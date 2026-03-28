import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mapping import ColumnMapping
from app.services.file_parser import STANDARD_COLUMNS


async def auto_map_columns(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    entity_type: str,
    headers: list[str],
) -> tuple[dict[str, str], list[str]]:
    """
    Query ColumnMapping for the tenant/entity_type and match against file headers.

    Standard columns (phone_number, owner_name, type) are always auto-recognized
    and mapped to themselves.

    Returns:
        mapped: dict of {original_name -> friendly_name} for recognized columns
        unmapped: list of column names that have no mapping
    """
    result = await db.execute(
        select(ColumnMapping).where(
            ColumnMapping.tenant_id == tenant_id,
            ColumnMapping.entity_type == entity_type,
        )
    )
    db_mappings: list[ColumnMapping] = list(result.scalars().all())

    # Build lookup: original_name (lowercase) -> friendly_name
    mapping_lookup: dict[str, str] = {
        m.original_name.lower(): m.friendly_name for m in db_mappings
    }

    mapped: dict[str, str] = {}
    unmapped: list[str] = []

    for header in headers:
        header_lower = header.lower()
        if header_lower in STANDARD_COLUMNS:
            # Standard columns map to themselves
            mapped[header] = header
        elif header_lower in mapping_lookup:
            mapped[header] = mapping_lookup[header_lower]
        else:
            unmapped.append(header)

    return mapped, unmapped
