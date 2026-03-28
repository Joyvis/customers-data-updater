import hashlib
from collections import defaultdict

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.batch import BatchRecord, BatchRecordStatus


def _dedup_group_id(phone_number: str, entity_type: str) -> str:
    """Generate a stable hash-based group ID from phone number and entity type."""
    key = f"{phone_number.strip()}:{entity_type.strip()}".lower()
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def detect_duplicates(records: list[BatchRecord]) -> dict[str, list[BatchRecord]]:
    """
    Group BatchRecord instances by (phone_number, entity_type).
    Returns only groups that contain 2 or more records.
    Keys are the dedup_group_id hash strings.
    """
    groups: dict[str, list[BatchRecord]] = defaultdict(list)
    for record in records:
        group_id = _dedup_group_id(record.phone_number, record.entity_type)
        groups[group_id].append(record)

    return {gid: recs for gid, recs in groups.items() if len(recs) >= 2}


async def apply_dedup_flags(
    db: AsyncSession, groups: dict[str, list[BatchRecord]]
) -> None:
    """
    Set dedup_group_id and status=DEDUP_REVIEW on all records that belong to a
    duplicate group. Flushes changes to the session but does not commit.
    """
    for group_id, records in groups.items():
        for record in records:
            record.dedup_group_id = group_id
            record.status = BatchRecordStatus.DEDUP_REVIEW
    await db.flush()
