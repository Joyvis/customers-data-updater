# Customers Data Updater

Multi-tenant SaaS platform for automated real estate data refresh via AI-powered WhatsApp conversations. Built for Brazilian real estate agencies.

**Flow:** Upload CSV/Excel → Async processing (dedup, column mapping, validation) → AI WhatsApp outreach to property owners → Download refreshed CSV

## Tech Stack

- **API:** Python 3.12, FastAPI, Pydantic v2
- **Database:** PostgreSQL (async SQLAlchemy 2.0), Alembic migrations
- **Queue:** Celery + Redis
- **Storage:** S3-compatible (AWS S3 / MinIO)
- **AI:** Claude API (conversation orchestration)
- **Messaging:** WhatsApp Business API

## Data Model

```mermaid
erDiagram
    Tenant ||--o{ TenantUser : "has users"
    Tenant ||--o{ Batch : "owns batches"
    Tenant ||--o{ ColumnMapping : "defines mappings"
    Tenant ||--o{ EntityTypeConfig : "configures types"
    Tenant ||--o{ OptOutList : "tracks opt-outs"
    Tenant ||--o{ UsageRecord : "tracks usage"

    Batch ||--o{ BatchRecord : "contains records"
    Batch ||--o{ BatchValidationError : "has errors"

    BatchRecord ||--o{ Conversation : "triggers conversations"

    Conversation ||--o{ Message : "exchanges messages"

    Tenant {
        uuid id PK
        string name
        string slug UK
        json settings
        timestamp created_at
        timestamp updated_at
    }

    TenantUser {
        uuid id PK
        uuid tenant_id FK
        string email UK
        string hashed_password
        string full_name
        enum role "admin | operator"
        bool is_active
        timestamp created_at
    }

    Batch {
        uuid id PK
        uuid tenant_id FK
        string file_name
        string file_key
        int file_size
        enum status "uploaded | queued | processing | review | approved | outreach | completed | partially_completed | failed"
        int total_records
        int processed_records
        int max_messages_per_conversation
        json settings
        timestamp created_at
        timestamp updated_at
    }

    BatchRecord {
        uuid id PK
        uuid batch_id FK
        uuid tenant_id FK
        int row_number
        string phone_number
        string owner_name
        string entity_type
        json original_data
        json updated_data
        enum status "pending | ready | dedup_review | outreach | completed | dead_letter | skipped | opted_out"
        string dedup_group_id
        json dedup_resolution
        timestamp created_at
        timestamp updated_at
    }

    BatchValidationError {
        uuid id PK
        uuid batch_id FK
        uuid tenant_id FK
        int row_number
        string error_type
        text message
        timestamp created_at
    }

    Conversation {
        uuid id PK
        uuid batch_record_id FK
        uuid tenant_id FK
        string phone_number
        enum status "ready | in_progress | completed | failed | cancelled"
        string classification
        int message_count
        int max_messages
        timestamp created_at
        timestamp updated_at
        timestamp completed_at
    }

    Message {
        uuid id PK
        uuid conversation_id FK
        uuid tenant_id FK
        enum direction "outbound | inbound"
        text content
        json ai_reasoning
        float classification_score
        json raw_payload
        timestamp created_at
    }

    ColumnMapping {
        uuid id PK
        uuid tenant_id FK
        string entity_type
        string original_name
        string friendly_name
        timestamp created_at
    }

    EntityTypeConfig {
        uuid id PK
        uuid tenant_id FK
        string entity_type
        json required_columns
        json settings
        timestamp created_at
    }

    OptOutList {
        uuid id PK
        uuid tenant_id FK
        string phone_number
        string reason
        timestamp created_at
    }

    UsageRecord {
        uuid id PK
        uuid tenant_id FK
        string event_type
        int count
        string period "YYYY-MM"
        json metadata
        timestamp created_at
    }
```

## Getting Started

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for PostgreSQL, Redis, MinIO)

### Setup

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run migrations
alembic upgrade head

# Start API
uvicorn app.main:app --reload

# Start Celery worker (separate terminal)
celery -A app.celery_app worker -l info
```

### Running Tests

```bash
pytest tests/ -v
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/login` | POST | JWT login |
| `/auth/register` | POST | Register user |
| `/auth/refresh` | POST | Refresh token |
| `/tenants/` | POST | Create tenant |
| `/batches/upload` | POST | Upload CSV/Excel |
| `/batches/{id}` | GET | Batch status |
| `/batches/{id}/errors` | GET | Validation errors |
| `/batches/{id}/dedup-groups` | GET | Dedup groups |
| `/batches/{id}/approve` | POST | Approve batch |
| `/batches/{id}/download` | GET | Download refreshed CSV |
| `/batches/{id}/dead-letter` | GET | Dead letter records |
| `/conversations/` | GET | List conversations |
| `/conversations/{id}` | GET | Conversation detail |
| `/webhooks/whatsapp` | POST | WhatsApp webhook |
| `/mappings/` | GET/PUT | Column mappings |
| `/erasure/phone/{phone}` | POST | LGPD data erasure |
| `/usage/` | GET | Usage summary |

## License

Proprietary
