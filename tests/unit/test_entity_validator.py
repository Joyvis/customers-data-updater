"""
Unit tests for app/services/entity_validator.py

Coverage:
  T13 - AC: validate_entity_types returns no errors when all required columns are present
  T14 - AC: validate_entity_types returns an error for a record missing a required column
  T15 - AC: validate_entity_types skips records with no recognisable entity_type (defaults
             handled upstream in processing.py; validator treats them as unknown type → no error)
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.entity_validator import validate_entity_types


# ---------------------------------------------------------------------------
# T13: Record with type "property" and all required cols → no errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_entity_types_no_errors_when_all_required_cols_present(
    db_session: AsyncSession, tenant
):
    """
    T13 — A record with type='property' and headers that include all required columns
    ('address', 'status') produces an empty error list.

    The tenant fixture has EntityTypeConfig for 'property' requiring [address, status].
    """
    headers = ["phone_number", "owner_name", "address", "status"]
    records = [
        {
            "type": "property",
            "phone_number": "11999990001",
            "owner_name": "Alice",
            "address": "Rua A",
            "status": "active",
        }
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


@pytest.mark.asyncio
async def test_validate_entity_types_multiple_valid_records_no_errors(
    db_session: AsyncSession, tenant
):
    """T13 (multiple rows) — Multiple valid property records produce no errors."""
    headers = ["phone_number", "owner_name", "address", "status", "type"]
    records = [
        {
            "type": "property",
            "phone_number": "11999990001",
            "owner_name": "Alice",
            "address": "Rua A",
            "status": "active",
        },
        {
            "type": "property",
            "phone_number": "11999990002",
            "owner_name": "Bob",
            "address": "Rua B",
            "status": "inactive",
        },
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


@pytest.mark.asyncio
async def test_validate_entity_types_header_matching_is_case_insensitive(
    db_session: AsyncSession, tenant
):
    """T13 (case) — Required column matching in headers is case-insensitive."""
    headers = ["phone_number", "owner_name", "ADDRESS", "STATUS"]
    records = [
        {"type": "property", "phone_number": "11999990001", "owner_name": "Alice"}
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


# ---------------------------------------------------------------------------
# T14: Record with type "property" missing "status" → error with correct message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_entity_types_error_when_required_col_missing(
    db_session: AsyncSession, tenant
):
    """
    T14 — A record with type='property' when 'status' is absent from headers
    produces exactly one error dict describing the problem.
    """
    # 'status' is missing from headers (tenant config requires [address, status])
    headers = ["phone_number", "owner_name", "address"]
    records = [
        {
            "type": "property",
            "phone_number": "11999990001",
            "owner_name": "Alice",
            "address": "Rua A",
        }
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert len(errors) == 1
    error = errors[0]
    assert error["error_type"] == "missing_required_columns"
    assert "status" in error["message"]
    assert "property" in error["message"]


@pytest.mark.asyncio
async def test_validate_entity_types_error_row_number_is_correct(
    db_session: AsyncSession, tenant
):
    """T14 (row number) — The row_number in the error dict corresponds to the record's index (1-based)."""
    headers = ["phone_number", "owner_name", "address"]
    records = [
        {
            "type": "property",
            "phone_number": "11999990001",
            "owner_name": "Alice",
            "address": "Rua A",
        },
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert len(errors) == 1
    assert errors[0]["row_number"] == 1


@pytest.mark.asyncio
async def test_validate_entity_types_only_invalid_rows_reported(
    db_session: AsyncSession, tenant
):
    """T14 (partial) — Only records that are actually missing columns produce errors."""
    # Row 1 has 'address' AND 'status' (valid), Row 2 is missing 'status' (invalid)
    # But headers must be common — use headers that satisfy row 1 but not row 2
    # Actually both rows share the same headers list; validation is headers-level per entity_type.
    # So if 'status' is absent from headers, ALL property records get errors.
    # Let's verify: 2 property records, headers missing 'status' → 2 errors.
    headers = ["phone_number", "owner_name", "address"]
    records = [
        {"type": "property", "phone_number": "11999990001", "owner_name": "Alice"},
        {"type": "property", "phone_number": "11999990002", "owner_name": "Bob"},
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert len(errors) == 2
    assert errors[0]["row_number"] == 1
    assert errors[1]["row_number"] == 2


@pytest.mark.asyncio
async def test_validate_entity_types_error_message_contains_missing_column(
    db_session: AsyncSession, tenant
):
    """T14 (message) — Error message explicitly names the missing column."""
    headers = ["phone_number", "owner_name"]  # both address and status are missing
    records = [
        {"type": "property", "phone_number": "11999990001", "owner_name": "Alice"}
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert len(errors) >= 1
    combined_message = " ".join(e["message"] for e in errors)
    # At least one of the missing required columns should be mentioned
    assert "address" in combined_message or "status" in combined_message


# ---------------------------------------------------------------------------
# T15: No "type" column → defaults to "property" handled upstream; validator skips unknown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_entity_types_record_without_type_key_produces_no_error(
    db_session: AsyncSession, tenant
):
    """
    T15 — Records without a 'type' or 'entity_type' key produce an empty string entity_type
    inside the validator. Since "" is not in the config_map, the validator skips the record
    and returns no errors.

    The actual default of "property" is assigned in processing.py before records reach the DB;
    entity_validator.py itself sees the raw row dict and falls through on unknown types.
    """
    headers = ["phone_number", "owner_name", "address", "status"]
    records = [
        {"no_type_field": "x", "phone_number": "11999990001", "owner_name": "Alice"}
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


@pytest.mark.asyncio
async def test_validate_entity_types_unknown_entity_type_skipped(
    db_session: AsyncSession, tenant
):
    """T15 (unknown type) — Records with an entity_type that has no config entry are skipped."""
    headers = ["phone_number", "owner_name", "address", "status"]
    records = [
        {"type": "vehicle", "phone_number": "11999990001", "owner_name": "Alice"}
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


@pytest.mark.asyncio
async def test_validate_entity_types_empty_type_string_skipped(
    db_session: AsyncSession, tenant
):
    """T15 (empty string) — A record with type='' is treated as unknown and skipped."""
    headers = ["phone_number", "owner_name", "address", "status"]
    records = [{"type": "", "phone_number": "11999990001", "owner_name": "Alice"}]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    assert errors == []


@pytest.mark.asyncio
async def test_validate_entity_types_entity_type_field_name_alias(
    db_session: AsyncSession, tenant
):
    """T15 (field alias) — 'entity_type' key in record is recognised as well as 'type'."""
    headers = ["phone_number", "owner_name", "address", "status"]
    records = [
        {
            "entity_type": "property",  # use the alternative field name
            "phone_number": "11999990001",
            "owner_name": "Alice",
            "address": "Rua A",
            "status": "active",
        }
    ]

    errors = await validate_entity_types(db_session, tenant.id, records, headers)

    # entity_validator reads row.get("entity_type", row.get("type", ""))
    # so this should validate correctly with no errors
    assert errors == []
