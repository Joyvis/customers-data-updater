"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-28
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tenants
    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("slug", sa.String(100), unique=True, nullable=False),
        sa.Column("settings", postgresql.JSON, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Tenant users
    op.create_table(
        "tenant_users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("email", sa.String(255), unique=True, nullable=False),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("role", sa.Enum("admin", "operator", name="userrole"), server_default="operator"),
        sa.Column("is_active", sa.Boolean, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Entity type configs
    op.create_table(
        "entity_type_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("entity_type", sa.String(100), nullable=False),
        sa.Column("required_columns", postgresql.JSON, nullable=False),
        sa.Column("settings", postgresql.JSON, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Column mappings
    op.create_table(
        "column_mappings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("entity_type", sa.String(100), nullable=False),
        sa.Column("original_name", sa.String(255), nullable=False),
        sa.Column("friendly_name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Batches
    op.create_table(
        "batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("file_name", sa.String(255), nullable=False),
        sa.Column("file_key", sa.String(500), nullable=False),
        sa.Column("file_size", sa.Integer, nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "uploaded", "queued", "processing", "review", "approved",
                "outreach", "completed", "partially_completed", "failed",
                name="batchstatus",
            ),
            server_default="uploaded",
        ),
        sa.Column("total_records", sa.Integer, server_default="0"),
        sa.Column("processed_records", sa.Integer, server_default="0"),
        sa.Column("max_messages_per_conversation", sa.Integer, server_default="5"),
        sa.Column("settings", postgresql.JSON, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Batch records
    op.create_table(
        "batch_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("batches.id"), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("row_number", sa.Integer, nullable=False),
        sa.Column("phone_number", sa.String(50), nullable=False),
        sa.Column("owner_name", sa.String(255), nullable=False),
        sa.Column("entity_type", sa.String(100), server_default="property"),
        sa.Column("original_data", postgresql.JSON, nullable=False),
        sa.Column("updated_data", postgresql.JSON, nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "pending", "ready", "dedup_review", "outreach",
                "completed", "dead_letter", "skipped", "opted_out",
                name="batchrecordstatus",
            ),
            server_default="pending",
        ),
        sa.Column("dedup_group_id", sa.String(100), nullable=True),
        sa.Column("dedup_resolution", postgresql.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Batch validation errors
    op.create_table(
        "batch_validation_errors",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("batches.id"), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("row_number", sa.Integer, nullable=False),
        sa.Column("error_type", sa.String(100), nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Conversations
    op.create_table(
        "conversations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("batch_record_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("batch_records.id"), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("phone_number", sa.String(50), nullable=False),
        sa.Column(
            "status",
            sa.Enum("ready", "in_progress", "completed", "failed", "cancelled", name="conversationstatus"),
            server_default="ready",
        ),
        sa.Column("classification", sa.String(50), nullable=True),
        sa.Column("message_count", sa.Integer, server_default="0"),
        sa.Column("max_messages", sa.Integer, server_default="5"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Messages
    op.create_table(
        "messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("conversations.id"), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column(
            "direction",
            sa.Enum("outbound", "inbound", name="messagedirection"),
            nullable=False,
        ),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("ai_reasoning", postgresql.JSON, nullable=True),
        sa.Column("classification_score", sa.Float, nullable=True),
        sa.Column("raw_payload", postgresql.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Opt-out list
    op.create_table(
        "opt_out_list",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("phone_number", sa.String(50), nullable=False),
        sa.Column("reason", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Usage records
    op.create_table(
        "usage_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("count", sa.Integer, server_default="1"),
        sa.Column("period", sa.String(7), nullable=False),
        sa.Column("metadata", postgresql.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Indexes for common queries
    op.create_index("ix_batch_records_batch_id", "batch_records", ["batch_id"])
    op.create_index("ix_batch_records_tenant_id", "batch_records", ["tenant_id"])
    op.create_index("ix_batch_records_phone_number", "batch_records", ["phone_number"])
    op.create_index("ix_batch_records_status", "batch_records", ["status"])
    op.create_index("ix_conversations_batch_record_id", "conversations", ["batch_record_id"])
    op.create_index("ix_conversations_tenant_id", "conversations", ["tenant_id"])
    op.create_index("ix_conversations_phone_number", "conversations", ["phone_number"])
    op.create_index("ix_messages_conversation_id", "messages", ["conversation_id"])
    op.create_index("ix_opt_out_list_tenant_phone", "opt_out_list", ["tenant_id", "phone_number"])
    op.create_index("ix_usage_records_tenant_period", "usage_records", ["tenant_id", "period"])


def downgrade() -> None:
    op.drop_table("usage_records")
    op.drop_table("opt_out_list")
    op.drop_table("messages")
    op.drop_table("conversations")
    op.drop_table("batch_validation_errors")
    op.drop_table("batch_records")
    op.drop_table("batches")
    op.drop_table("column_mappings")
    op.drop_table("entity_type_configs")
    op.drop_table("tenant_users")
    op.drop_table("tenants")

    op.execute("DROP TYPE IF EXISTS userrole")
    op.execute("DROP TYPE IF EXISTS batchstatus")
    op.execute("DROP TYPE IF EXISTS batchrecordstatus")
    op.execute("DROP TYPE IF EXISTS conversationstatus")
    op.execute("DROP TYPE IF EXISTS messagedirection")
