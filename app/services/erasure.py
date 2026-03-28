import uuid

from sqlalchemy import CursorResult, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.batch import BatchRecord
from app.models.conversation import Conversation, Message
from app.models.opt_out import OptOutList


async def erase_data_for_phone(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    phone_number: str,
) -> dict:
    """
    LGPD erasure: delete all data associated with a phone number for a tenant.

    Deletes in dependency order:
      1. Messages belonging to conversations for this phone + tenant
      2. Conversations for this phone + tenant
      3. BatchRecords for this phone + tenant
      4. OptOutList entries for this phone + tenant

    Returns counts of deleted rows.
    """
    # Step 1 — find conversation ids for this phone + tenant
    conv_result = await db.execute(
        select(Conversation.id).where(
            Conversation.tenant_id == tenant_id,
            Conversation.phone_number == phone_number,
        )
    )
    conversation_ids = [row[0] for row in conv_result.all()]

    # Step 2 — delete messages belonging to those conversations
    messages_deleted = 0
    if conversation_ids:
        msg_result: CursorResult = await db.execute(
            delete(Message).where(Message.conversation_id.in_(conversation_ids))
        )
        messages_deleted = msg_result.rowcount  # type: ignore[assignment]

    # Step 3 — delete conversations
    conv_del_result: CursorResult = await db.execute(
        delete(Conversation).where(
            Conversation.tenant_id == tenant_id,
            Conversation.phone_number == phone_number,
        )
    )
    conversations_deleted = conv_del_result.rowcount  # type: ignore[assignment]

    # Step 4 — delete batch records
    record_del_result: CursorResult = await db.execute(
        delete(BatchRecord).where(
            BatchRecord.tenant_id == tenant_id,
            BatchRecord.phone_number == phone_number,
        )
    )
    records_deleted = record_del_result.rowcount  # type: ignore[assignment]

    # Step 5 — delete opt-out list entries
    await db.execute(
        delete(OptOutList).where(
            OptOutList.tenant_id == tenant_id,
            OptOutList.phone_number == phone_number,
        )
    )

    await db.commit()

    return {
        "messages_deleted": messages_deleted,
        "conversations_deleted": conversations_deleted,
        "records_deleted": records_deleted,
    }
