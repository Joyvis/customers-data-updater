import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class BatchStatus(str, enum.Enum):
    UPLOADED = "uploaded"
    QUEUED = "queued"
    PROCESSING = "processing"
    REVIEW = "review"
    APPROVED = "approved"
    OUTREACH = "outreach"
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    FAILED = "failed"


class BatchRecordStatus(str, enum.Enum):
    PENDING = "pending"
    READY = "ready"
    DEDUP_REVIEW = "dedup_review"
    OUTREACH = "outreach"
    COMPLETED = "completed"
    DEAD_LETTER = "dead_letter"
    SKIPPED = "skipped"
    OPTED_OUT = "opted_out"


class Batch(Base):
    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_key: Mapped[str] = mapped_column(String(500), nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[BatchStatus] = mapped_column(
        Enum(BatchStatus), default=BatchStatus.UPLOADED
    )
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    processed_records: Mapped[int] = mapped_column(Integer, default=0)
    max_messages_per_conversation: Mapped[int] = mapped_column(Integer, default=5)
    settings: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    tenant: Mapped["Tenant"] = relationship(back_populates="batches")  # noqa: F821
    records: Mapped[list["BatchRecord"]] = relationship(
        back_populates="batch", cascade="all, delete-orphan"
    )
    validation_errors: Mapped[list["BatchValidationError"]] = relationship(
        back_populates="batch", cascade="all, delete-orphan"
    )


class BatchRecord(Base):
    __tablename__ = "batch_records"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("batches.id"), nullable=False
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    phone_number: Mapped[str] = mapped_column(String(50), nullable=False)
    owner_name: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), default="property")
    original_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    updated_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[BatchRecordStatus] = mapped_column(
        Enum(BatchRecordStatus), default=BatchRecordStatus.PENDING
    )
    dedup_group_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    dedup_resolution: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    batch: Mapped["Batch"] = relationship(back_populates="records")
    conversations: Mapped[list["Conversation"]] = relationship(
        back_populates="batch_record", cascade="all, delete-orphan"
    )  # noqa: F821


class BatchValidationError(Base):
    __tablename__ = "batch_validation_errors"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("batches.id"), nullable=False
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    error_type: Mapped[str] = mapped_column(String(100), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    batch: Mapped["Batch"] = relationship(back_populates="validation_errors")
