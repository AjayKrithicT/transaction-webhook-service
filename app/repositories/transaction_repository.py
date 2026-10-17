
import logging
from time import perf_counter

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transaction import Transaction
from app.schemas.transaction import TransactionWebhookRequest


logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """Raised when a transaction ID is reused with a different payload."""


async def create_transaction(
    db: AsyncSession,
    payload: TransactionWebhookRequest,
) -> tuple[Transaction, bool]:
    """Accept a transaction using one database function call."""

    started_at = perf_counter()

    try:
        result = await db.execute(
            text("""
                SELECT *
                FROM public.accept_transaction_webhook(
                    :transaction_id,
                    :source_account,
                    :destination_account,
                    :amount,
                    :currency
                )
            """),
            {
                "transaction_id": payload.transaction_id,
                "source_account": payload.source_account,
                "destination_account": payload.destination_account,
                "amount": payload.amount,
                "currency": payload.currency,
            },
        )

        row = result.mappings().one()

        if row["result_conflict"]:
            await db.rollback()
            raise IdempotencyConflictError(
                "Transaction ID already exists with a different payload."
            )

        await db.commit()

        elapsed_ms = (perf_counter() - started_at) * 1000
        logger.info(
            "Database acceptance call and commit: %.2f ms",
            elapsed_ms,
        )

        transaction = Transaction(
            transaction_id=row["result_transaction_id"],
            status=row["result_status"],
            source_account=payload.source_account,
            destination_account=payload.destination_account,
            amount=payload.amount,
            currency=payload.currency,
        )

        return transaction, row["result_created"]

    except IdempotencyConflictError:
        raise
    except Exception:
        await db.rollback()
        raise


async def get_transaction(
    db: AsyncSession,
    transaction_id: str,
) -> Transaction | None:
    """Retrieve a transaction by its ID."""

    return await db.get(Transaction, transaction_id)
