from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    CheckConstraint,
    DateTime,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import TransactionStatus
from app.db.base import Base


class Transaction(Base):
    __tablename__ = "transactions"

    __table_args__ = (
        CheckConstraint(
            "amount > 0",
            name="ck_transactions_amount",
        ),
        CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name="ck_transactions_currency",
        ),
        CheckConstraint(
            "status IN ('PROCESSING', 'PROCESSED', 'FAILED')",
            name="ck_transactions_status",
        ),
        CheckConstraint(
            """
            (status = 'PROCESSED' AND processed_at IS NOT NULL)
            OR
            (status IN ('PROCESSING', 'FAILED')
             AND processed_at IS NULL)
            """,
            name="ck_transactions_status_timestamp",
        ),
    )

    transaction_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
    )

    source_account: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    destination_account: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
    )

    currency: Mapped[str] = mapped_column(
        CHAR(3),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default=TransactionStatus.PROCESSING.value,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )