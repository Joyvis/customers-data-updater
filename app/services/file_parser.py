import csv
import io

import chardet
import openpyxl


REQUIRED_COLUMNS = {"phone_number", "owner_name"}
STANDARD_COLUMNS = {"phone_number", "owner_name", "type"}


def detect_encoding(content: bytes) -> str:
    """Detect the character encoding of byte content. Falls back to latin-1."""
    result = chardet.detect(content)
    encoding = result.get("encoding")
    if not encoding:
        return "latin-1"
    return encoding


def parse_csv(content: bytes) -> tuple[list[str], list[dict]]:
    """Parse CSV bytes into (headers, rows). Detects encoding automatically."""
    encoding = detect_encoding(content)
    try:
        text = content.decode(encoding)
    except (UnicodeDecodeError, LookupError):
        text = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    headers: list[str] = list(reader.fieldnames or [])
    rows: list[dict] = [dict(row) for row in reader]
    return headers, rows


def parse_excel(content: bytes) -> tuple[list[str], list[dict]]:
    """Parse Excel (.xlsx) bytes into (headers, rows) using openpyxl read_only mode."""
    workbook = openpyxl.load_workbook(
        filename=io.BytesIO(content), read_only=True, data_only=True
    )
    sheet = workbook.active

    rows_iter = sheet.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration:
        workbook.close()
        return [], []

    headers: list[str] = [str(cell) if cell is not None else "" for cell in header_row]
    rows: list[dict] = []
    for raw_row in rows_iter:
        row_dict = {
            headers[i]: (raw_row[i] if i < len(raw_row) else None)
            for i in range(len(headers))
        }
        rows.append(row_dict)

    workbook.close()
    return headers, rows


def parse_file(filename: str, content: bytes) -> tuple[list[str], list[dict]]:
    """Dispatch parsing by file extension (.csv or .xlsx)."""
    lower = filename.lower()
    if lower.endswith(".xlsx"):
        return parse_excel(content)
    if lower.endswith(".csv"):
        return parse_csv(content)
    raise ValueError(
        f"Unsupported file type: {filename}. Only .csv and .xlsx are accepted."
    )


def validate_required_columns(headers: list[str]) -> None:
    """Raise ValueError if phone_number or owner_name is not present in headers."""
    lower_headers = {h.lower() for h in headers}
    missing = REQUIRED_COLUMNS - lower_headers
    if missing:
        raise ValueError(
            f"Missing required columns: {', '.join(sorted(missing))}. "
            f"The file must contain: {', '.join(sorted(REQUIRED_COLUMNS))}."
        )
