import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mapping import EntityTypeConfig


async def validate_entity_types(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    records: list[dict],
    headers: list[str],
) -> list[dict]:
    """
    Validate that each record's entity_type has all its required columns present
    in the file headers.

    Loads EntityTypeConfig for the tenant and checks per record.

    Returns a list of error dicts:
        {row_number, error_type, message}
    """
    result = await db.execute(
        select(EntityTypeConfig).where(EntityTypeConfig.tenant_id == tenant_id)
    )
    configs: list[EntityTypeConfig] = list(result.scalars().all())
    config_map: dict[str, list[str]] = {
        c.entity_type: c.required_columns for c in configs
    }

    lower_headers = {h.lower() for h in headers}
    errors: list[dict] = []

    for idx, row in enumerate(records, start=1):
        entity_type = str(row.get("entity_type", row.get("type", ""))).strip()
        if not entity_type or entity_type not in config_map:
            continue

        required: list[str] = config_map[entity_type]
        missing = [col for col in required if col.lower() not in lower_headers]
        if missing:
            errors.append(
                {
                    "row_number": idx,
                    "error_type": "missing_required_columns",
                    "message": (
                        f"Type '{entity_type}' requires columns: "
                        f"{', '.join(required)}. "
                        f"Missing: {', '.join(missing)}."
                    ),
                }
            )

    return errors
