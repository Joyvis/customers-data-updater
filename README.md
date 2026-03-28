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

## Data Flow

```mermaid
flowchart TB
    subgraph Upload["1 - Upload"]
        A[/"CSV / Excel file"/] -->|POST /batches/upload| B[Validate headers & size]
        B -->|phone_number + owner_name required| C[Store file in S3]
        C --> D[Create Batch<br/>status: UPLOADED]
        D -->|Celery task queued| E[[process_batch_task]]
    end

    subgraph Processing["2 - Async Processing (Celery Worker)"]
        E --> F[Download from S3]
        F --> G[Parse file<br/>detect encoding<br/>UTF-8 / Latin-1]
        G --> H[Create BatchRecords<br/>in chunks of 100]
        H --> I[Entity type validation<br/>check required columns<br/>per EntityTypeConfig]
        I --> J[Deduplication<br/>group by phone + entity_type]
        J --> K[Column auto-mapping<br/>from ColumnMapping dictionary]
        K --> L[Batch status: REVIEW]
    end

    subgraph Review["3 - Human Review"]
        L --> M{Issues to resolve?}
        M -->|Validation errors| M1[Review errors<br/>GET /batches/id/errors]
        M -->|Dedup groups| M2[Merge or skip duplicates<br/>POST .../dedup-groups/id/resolve]
        M -->|Unmapped columns| M3[Assign friendly names<br/>PUT /mappings]
        M -->|Recently refreshed| M4[Skip or re-refresh<br/>GET .../recently-refreshed]
        M1 & M2 & M3 & M4 --> N[POST /batches/id/approve]
    end

    subgraph Approval["4 - Approval"]
        N --> O{Phone opted out?}
        O -->|Yes| P[Record status: OPTED_OUT]
        O -->|No| Q[Create Conversation<br/>status: READY]
        Q --> R[[send_initial_outreach_task]]
    end

    subgraph Outreach["5 - AI WhatsApp Outreach (Celery Worker)"]
        R --> S[Send template message<br/>via WhatsApp Business API]
        S --> T[Conversation status: IN_PROGRESS]
        T --> U[/Owner replies via WhatsApp/]
        U -->|Webhook POST /webhooks/whatsapp| V[Claude API processes response]
        V --> W{Classification}
        W -->|confirmed| X[Copy original_data to updated_data<br/>Conversation: COMPLETED]
        W -->|updated| Y[Extract changes to updated_data<br/>Conversation: COMPLETED]
        W -->|unclear| Z{Message limit<br/>reached?}
        Z -->|No| AA[Send follow-up message] --> U
        Z -->|Yes| AB[Dead letter queue<br/>Conversation: FAILED]
        W -->|opt-out| AC[Add to OptOutList<br/>Conversation: CANCELLED]
        AB & AC --> AD[Record status: DEAD_LETTER]
    end

    subgraph Completion["6 - Export"]
        X & Y --> AE[Record status: COMPLETED]
        AE & AD & P --> AF{All conversations<br/>finished?}
        AF -->|All success| AG[Batch: COMPLETED]
        AF -->|Some dead letters| AH[Batch: PARTIALLY_COMPLETED]
        AG & AH --> AI[GET /batches/id/download]
        AI --> AJ[Generate CSV/Excel<br/>friendly column names<br/>updated values<br/>status column]
        AJ --> AK[/"Refreshed CSV / Excel file"/]
    end

    style Upload fill:#e8f4fd,stroke:#1a73e8
    style Processing fill:#fef7e0,stroke:#f9a825
    style Review fill:#fce4ec,stroke:#e53935
    style Approval fill:#e8f5e9,stroke:#43a047
    style Outreach fill:#f3e5f5,stroke:#8e24aa
    style Completion fill:#e0f2f1,stroke:#00897b
```

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
