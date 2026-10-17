from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import JobStatus
from app.db.base import Base


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    __table_args__ = (
        UniqueConstraint(
            "transaction_id",
            name="processing_jobs_transaction_id_key",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')",
            name="ck_processing_jobs_status",
        ),
        CheckConstraint(
            "attempts >= 0",
            name="ck_processing_jobs_attempts",
        ),
        CheckConstraint(
            "max_attempts > 0",
            name="ck_processing_jobs_max_attempts",
        ),
        Index(
            "idx_processing_jobs_pending",
            "available_at",
            "created_at",
            postgresql_where=text("status = 'PENDING'"),
        ),
        Index(
            "idx_processing_jobs_expired_leases",
            "locked_until",
            postgresql_where=text("status = 'PROCESSING'"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    transaction_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("transactions.transaction_id"),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default=JobStatus.PENDING.value,
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="0",
    )

    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="3",
    )

    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    locked_by: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )