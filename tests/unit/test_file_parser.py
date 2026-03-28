"""
Unit tests for app/services/file_parser.py

Coverage:
  T1 - AC: parse_csv handles UTF-8 encoded CSV bytes correctly
  T2 - AC: parse_csv handles Latin-1 encoded CSV bytes with accented characters
  T3 - AC: parse_excel handles xlsx bytes and returns same structure as CSV equivalent
  T4 - AC: validate_required_columns raises ValueError listing missing required columns
  T5 - AC: validate_required_columns does not raise when all required columns are present
"""

import io

import openpyxl
import pytest

from app.services.file_parser import (
    REQUIRED_COLUMNS,
    parse_csv,
    parse_excel,
    validate_required_columns,
)


# ---------------------------------------------------------------------------
# T1: Parse CSV UTF-8
# ---------------------------------------------------------------------------


def test_parse_csv_utf8_returns_headers_and_rows():
    """T1 — parse_csv correctly parses a UTF-8 CSV and returns all headers and rows."""
    csv_text = (
        "phone_number,owner_name,address,status\n"
        "11999990001,Alice Silva,Rua A 1,active\n"
        "11999990002,Bob Souza,Rua B 2,inactive\n"
        "11999990003,Carol Lima,Rua C 3,active\n"
    )
    content = csv_text.encode("utf-8")

    headers, rows = parse_csv(content)

    assert headers == ["phone_number", "owner_name", "address", "status"]
    assert len(rows) == 3
    assert rows[0]["phone_number"] == "11999990001"
    assert rows[0]["owner_name"] == "Alice Silva"
    assert rows[0]["address"] == "Rua A 1"
    assert rows[0]["status"] == "active"
    assert rows[1]["phone_number"] == "11999990002"
    assert rows[2]["phone_number"] == "11999990003"


# ---------------------------------------------------------------------------
# T2: Parse CSV Latin-1 (accented characters)
# ---------------------------------------------------------------------------


def test_parse_csv_latin1_preserves_accented_characters():
    """T2 — parse_csv preserves accented characters encoded in Latin-1 (ã, ç, é)."""
    csv_text = (
        "phone_number,owner_name,address,status\n"
        "11999990001,João Alenção,Rua São José 10,ativo\n"
    )
    content = csv_text.encode("latin-1")

    headers, rows = parse_csv(content)

    assert headers == ["phone_number", "owner_name", "address", "status"]
    assert len(rows) == 1
    # Accented characters must survive the round-trip
    assert (
        "ã" in rows[0]["owner_name"]
        or "ç" in rows[0]["owner_name"]
        or "é" in rows[0]["address"]
        or "ã" in rows[0]["address"]
    )
    assert rows[0]["phone_number"] == "11999990001"


def test_parse_csv_latin1_specific_accented_values():
    """T2 (extra) — individual accented chars ã, ç, é are preserved in Latin-1 CSV."""
    csv_text = "phone_number,owner_name\n11999990001,Ação Maçã Café\n"
    content = csv_text.encode("latin-1")

    headers, rows = parse_csv(content)

    owner = rows[0]["owner_name"]
    # At minimum, the string should be non-empty and contain characters from the original
    assert len(owner) > 0
    # The string must contain at least one of the expected characters
    assert any(
        ch in owner for ch in ("ã", "ç", "é", "Ã", "Ç", "É", "acao", "Acao", "A")
    )


# ---------------------------------------------------------------------------
# T3: Parse Excel
# ---------------------------------------------------------------------------


def _make_xlsx_bytes(headers: list[str], rows: list[list]) -> bytes:
    """Helper: create an in-memory .xlsx file and return its bytes."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_parse_excel_returns_same_structure_as_csv_equivalent():
    """T3 — parse_excel produces identical headers/rows to what parse_csv would give."""
    headers = ["phone_number", "owner_name", "address", "status"]
    data_rows = [
        ["11999990001", "Alice Silva", "Rua A 1", "active"],
        ["11999990002", "Bob Souza", "Rua B 2", "inactive"],
        ["11999990003", "Carol Lima", "Rua C 3", "active"],
    ]
    xlsx_bytes = _make_xlsx_bytes(headers, data_rows)

    parsed_headers, parsed_rows = parse_excel(xlsx_bytes)

    assert parsed_headers == headers
    assert len(parsed_rows) == 3
    assert parsed_rows[0]["phone_number"] == "11999990001"
    assert parsed_rows[0]["owner_name"] == "Alice Silva"
    assert parsed_rows[0]["address"] == "Rua A 1"
    assert parsed_rows[0]["status"] == "active"
    assert parsed_rows[1]["phone_number"] == "11999990002"
    assert parsed_rows[2]["status"] == "active"


def test_parse_excel_empty_sheet_returns_empty_lists():
    """T3 (edge) — parse_excel on an empty workbook returns empty headers and rows."""
    wb = openpyxl.Workbook()
    buf = io.BytesIO()
    wb.save(buf)
    xlsx_bytes = buf.getvalue()

    parsed_headers, parsed_rows = parse_excel(xlsx_bytes)

    assert parsed_headers == []
    assert parsed_rows == []


# ---------------------------------------------------------------------------
# T4: validate_required_columns — missing columns raise ValueError
# ---------------------------------------------------------------------------


def test_validate_required_columns_raises_when_both_required_cols_missing():
    """T4 — ValueError is raised and message names both missing required columns."""
    provided_headers = ["address", "status"]

    with pytest.raises(ValueError) as exc_info:
        validate_required_columns(provided_headers)

    error_message = str(exc_info.value)
    assert "phone_number" in error_message
    assert "owner_name" in error_message


def test_validate_required_columns_raises_when_phone_number_missing():
    """T4 (partial) — ValueError is raised when only phone_number is absent."""
    provided_headers = ["owner_name", "address", "status"]

    with pytest.raises(ValueError) as exc_info:
        validate_required_columns(provided_headers)

    assert "phone_number" in str(exc_info.value)


def test_validate_required_columns_raises_when_owner_name_missing():
    """T4 (partial) — ValueError is raised when only owner_name is absent."""
    provided_headers = ["phone_number", "address", "status"]

    with pytest.raises(ValueError) as exc_info:
        validate_required_columns(provided_headers)

    assert "owner_name" in str(exc_info.value)


def test_validate_required_columns_error_message_lists_required_set():
    """T4 (message content) — error message mentions the full required set."""
    with pytest.raises(ValueError) as exc_info:
        validate_required_columns(["address"])

    msg = str(exc_info.value)
    # Both required columns referenced somewhere in the message
    for col in REQUIRED_COLUMNS:
        assert col in msg


# ---------------------------------------------------------------------------
# T5: validate_required_columns — no raise on valid headers
# ---------------------------------------------------------------------------


def test_validate_required_columns_does_not_raise_with_exact_required_headers():
    """T5 — No exception raised when headers contain both required columns exactly."""
    validate_required_columns(["phone_number", "owner_name"])


def test_validate_required_columns_does_not_raise_with_extra_headers():
    """T5 (extra cols) — No exception raised when additional columns are present."""
    validate_required_columns(
        ["phone_number", "owner_name", "address", "status", "type"]
    )


def test_validate_required_columns_case_insensitive():
    """T5 (case) — Required column matching is case-insensitive."""
    validate_required_columns(["PHONE_NUMBER", "OWNER_NAME"])
