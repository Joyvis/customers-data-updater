import asyncio
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.celery_app import celery
from app.config import settings

logger = logging.getLogger(__name__)


def _get_async_session_factory() -> async_sessionmaker:
    """Create an async session factory bound to the async database URL.

    Celery workers use a sync context, so we manage the async event loop manually.
    Using the async engine ensures compatibility with the same asyncpg driver used
    by the FastAPI application.
    """
    engine = create_async_engine(settings.database_url, echo=False)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@celery.task(
    name="outreach.send_initial_outreach",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def send_initial_outreach_task(self, batch_id: str) -> None:
    """Celery task: start outreach for all READY conversations in a batch.

    Args:
        batch_id: String UUID of the batch to process.
    """
    from app.services.outreach import start_batch_outreach

    async def _run():
        factory = _get_async_session_factory()
        async with factory() as db:
            await start_batch_outreach(db, uuid.UUID(batch_id))

    try:
        asyncio.run(_run())
    except Exception as exc:
        logger.error(
            "send_initial_outreach_task failed for batch %s: %s", batch_id, exc
        )
        raise self.retry(exc=exc)


@celery.task(
    name="outreach.process_inbound_message",
    bind=True,
    max_retries=3,
    default_retry_delay=30,
)
def process_inbound_message_task(
    self,
    conversation_id: str,
    message_content: str,
    raw_payload: dict,
) -> None:
    """Celery task: process an inbound WhatsApp message for a conversation.

    Args:
        conversation_id: String UUID of the conversation.
        message_content: Text content of the owner's message.
        raw_payload: Raw webhook payload dict for audit trail.
    """
    from app.services.outreach import process_inbound_message

    async def _run():
        factory = _get_async_session_factory()
        async with factory() as db:
            await process_inbound_message(
                db,
                uuid.UUID(conversation_id),
                message_content,
                raw_payload,
            )

    try:
        asyncio.run(_run())
    except Exception as exc:
        logger.error(
            "process_inbound_message_task failed for conversation %s: %s",
            conversation_id,
            exc,
        )
        raise self.retry(exc=exc)
