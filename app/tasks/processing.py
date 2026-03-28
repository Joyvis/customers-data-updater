import asyncio
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.celery_app import celery
from app.config import settings


def _get_sync_session() -> Session:
    """Create a synchronous SQLAlchemy session from the sync database URL."""
    engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return SessionLocal()


@celery.task(
    name="tasks.process_batch", bind=True, max_retries=3, default_retry_delay=30
)
def process_batch_task(self, batch_id: str) -> None:
    """
    Celery task that wraps the async process_batch pipeline in a synchronous context.

    Uses an async event loop with a sync-compatible SQLAlchemy async session so the
    existing async service layer can run inside Celery workers.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.processing import process_batch

    batch_uuid = uuid.UUID(batch_id)

    async def _run():
        async_engine = create_async_engine(settings.database_url)
        async_session_factory = async_sessionmaker(
            async_engine, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session_factory() as session:
            await process_batch(session, batch_uuid)
        await async_engine.dispose()

    try:
        asyncio.run(_run())
    except Exception as exc:
        raise self.retry(exc=exc)
