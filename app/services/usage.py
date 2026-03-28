import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.usage import UsageRecord

VALID_EVENT_TYPES = {
    "record_processed",
    "conversation_started",
    "message_sent",
    "message_received",
    "api_call_made",
}


def _current_period() -> str:
    """Return the current period string in YYYY-MM format."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


async def record_event(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    event_type: str,
    count: int = 1,
) -> None:
    """
    Upsert a usage event into UsageRecord for the current period (YYYY-MM).

    If a record already exists for tenant + event_type + period, increment its
    count by the given amount.  Otherwise create a new record.
    """
    period = _current_period()

    result = await db.execute(
        select(UsageRecord).where(
            UsageRecord.tenant_id == tenant_id,
            UsageRecord.event_type == event_type,
            UsageRecord.period == period,
        )
    )
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.count += count
    else:
        db.add(
            UsageRecord(
                tenant_id=tenant_id,
                event_type=event_type,
                count=count,
                period=period,
            )
        )

    await db.flush()


async def get_usage_summary(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    period: str | None = None,
) -> list[dict]:
    """
    Return aggregated usage for a tenant.

    If period is provided (YYYY-MM), filter to that month only.
    Returns a list of dicts with keys: event_type, count, period.
    """
    query = select(UsageRecord).where(UsageRecord.tenant_id == tenant_id)
    if period is not None:
        query = query.where(UsageRecord.period == period)
    query = query.order_by(UsageRecord.period, UsageRecord.event_type)

    result = await db.execute(query)
    records: list[UsageRecord] = list(result.scalars().all())

    return [
        {"event_type": r.event_type, "count": r.count, "period": r.period}
        for r in records
    ]
