"""
Unit tests for usage tracking and aggregation.

T28: AC-Usage-1 — Usage counts match actual recorded events after aggregation
"""

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Tenant
from app.services.usage import get_usage_summary, record_event, _current_period


# ---------------------------------------------------------------------------
# T28: Usage counts match actual records/conversations/messages
# AC-Usage-1: record_event must persist usage and get_usage_summary must
#             return the correct aggregated count per event_type.
# ---------------------------------------------------------------------------


async def test_t28_usage_counts_match_after_multiple_record_events(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T28 — AC-Usage-1: Multiple record_event calls aggregate correctly."""
    # Record 100 processed records in one call
    await record_event(db_session, tenant.id, "record_processed", count=100)
    # Record 50 more
    await record_event(db_session, tenant.id, "record_processed", count=50)
    # Record a different event type
    await record_event(db_session, tenant.id, "conversation_started", count=10)

    summary = await get_usage_summary(db_session, tenant.id)

    by_type = {entry["event_type"]: entry["count"] for entry in summary}
    assert by_type["record_processed"] == 150
    assert by_type["conversation_started"] == 10


async def test_t28_usage_summary_filters_by_period(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T28 — AC-Usage-1: get_usage_summary with period= returns only that month's data."""
    current_period = _current_period()

    await record_event(db_session, tenant.id, "message_sent", count=5)

    summary_current = await get_usage_summary(
        db_session, tenant.id, period=current_period
    )
    assert len(summary_current) >= 1
    by_type = {e["event_type"]: e["count"] for e in summary_current}
    assert by_type["message_sent"] == 5

    # A non-existent past period should return nothing
    summary_past = await get_usage_summary(db_session, tenant.id, period="2000-01")
    assert summary_past == []


async def test_t28_usage_record_event_creates_new_row_for_new_event_type(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T28 edge case — A new event_type creates a distinct UsageRecord row."""
    await record_event(db_session, tenant.id, "api_call_made", count=3)
    await record_event(db_session, tenant.id, "message_received", count=7)

    summary = await get_usage_summary(db_session, tenant.id)
    by_type = {e["event_type"]: e["count"] for e in summary}

    assert by_type["api_call_made"] == 3
    assert by_type["message_received"] == 7


async def test_t28_usage_scoped_per_tenant(
    db_session: AsyncSession,
    tenant: Tenant,
    tenant_b: Tenant,
):
    """T28 edge case — Usage records from different tenants are isolated."""
    await record_event(db_session, tenant.id, "record_processed", count=200)
    await record_event(db_session, tenant_b.id, "record_processed", count=999)

    summary_a = await get_usage_summary(db_session, tenant.id)
    summary_b = await get_usage_summary(db_session, tenant_b.id)

    by_type_a = {e["event_type"]: e["count"] for e in summary_a}
    by_type_b = {e["event_type"]: e["count"] for e in summary_b}

    assert by_type_a["record_processed"] == 200
    assert by_type_b["record_processed"] == 999


async def test_t28_usage_summary_period_key_format(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T28 edge case — Each summary entry has correct keys including 'period'."""
    await record_event(db_session, tenant.id, "conversation_started")

    summary = await get_usage_summary(db_session, tenant.id)

    assert len(summary) >= 1
    entry = summary[0]
    assert "event_type" in entry
    assert "count" in entry
    assert "period" in entry
    # Period must match YYYY-MM format
    import re

    assert re.match(r"^\d{4}-\d{2}$", entry["period"]) is not None


async def test_t28_usage_count_increments_idempotently(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T28 edge case — Calling record_event multiple times for same type in same period increments."""
    for _ in range(5):
        await record_event(db_session, tenant.id, "message_sent", count=1)

    summary = await get_usage_summary(db_session, tenant.id)
    by_type = {e["event_type"]: e["count"] for e in summary}
    assert by_type["message_sent"] == 5
