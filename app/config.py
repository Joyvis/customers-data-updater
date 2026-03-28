from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "CDU_"}

    # Database
    database_url: str = "postgresql+asyncpg://cdu:cdu_dev_password@localhost:5432/customers_data_updater"
    database_url_sync: str = (
        "postgresql://cdu:cdu_dev_password@localhost:5432/customers_data_updater"
    )

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # S3 / MinIO
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "uploads"
    s3_region: str = "us-east-1"

    # JWT
    jwt_secret: str = "dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7

    # Claude API
    anthropic_api_key: str = ""

    # WhatsApp Business API
    whatsapp_api_url: str = "https://graph.facebook.com/v18.0"
    whatsapp_phone_number_id: str = ""
    whatsapp_access_token: str = ""
    whatsapp_app_secret: str = ""
    whatsapp_verify_token: str = "dev-verify-token"

    # Processing
    chunk_size: int = 100
    max_file_size_mb: int = 50
    default_max_messages_per_conversation: int = 5
    recently_refreshed_days: int = 7

    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"


settings = Settings()
