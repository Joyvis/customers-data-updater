from celery import Celery

from app.config import settings

celery = Celery(
    "customers_data_updater",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/Sao_Paulo",
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)
