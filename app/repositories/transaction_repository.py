
import logging
from time import perf_counter

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus, TransactionStatus
from app.models.processing_job import ProcessingJob
from app.models.transaction import Transaction
from app.schemas.transaction import TransactionWebhookRequest


logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """Raised when a transaction ID is reused with a different payload."""


async def create_transaction(
    db: AsyncSession,
    payload: TransactionWebhookRequest,
) -> tuple[Transaction, bool]:
    """Create a transaction and its processing job if it is new."""

    # INSERT the transaction and return its fields if newly created.
    insert_started = perf_counter()

    statement = (
        insert(Transaction)
        .values(
            transaction_id=payload.transaction_id,
            source_account=payload.source_account,
            destination_account=payload.destination_account,
            amount=payload.amount,
            currency=payload.currency,
            status=TransactionStatus.PROCESSING.value,
        )
        .on_conflict_do_nothing(
            index_elements=[Transaction.transaction_id]
        )
        .returning(Transaction)
    )

    result = await db.execute(statement)
    transaction = result.scalar_one_or_none()

    insert_ms = (perf_counter() - insert_started) * 1000
    logger.info("DB INSERT duration: %.2f ms", insert_ms)

    if transaction is not None:
        # New transaction: create its job in the same database transaction.
        db.add(
            ProcessingJob(
                transaction_id=transaction.transaction_id,
                status=JobStatus.PENDING.value,
            )
        )
        created = True

    else:
        # Existing transaction: fetch it and validate idempotency.
        select_started = perf_counter()

        result = await db.execute(
            select(Transaction).where(
                Transaction.transaction_id == payload.transaction_id
            )
        )
        transaction = result.scalar_one()

        select_ms = (perf_counter() - select_started) * 1000
        logger.info("DB SELECT duration: %.2f ms", select_ms)

        payload_matches = (
            transaction.source_account == payload.source_account
            and transaction.destination_account
            == payload.destination_account
            and transaction.amount == payload.amount
            and transaction.currency == payload.currency
        )

        if not payload_matches:
            await db.rollback()
            raise IdempotencyConflictError(
                "Transaction ID already exists with a different payload."
            )

        created = False

    # Commit the transaction and any newly created processing job.
    commit_started = perf_counter()
    await db.commit()

    commit_ms = (perf_counter() - commit_started) * 1000
    logger.info("DB COMMIT duration: %.2f ms", commit_ms)

    total_ms = insert_ms + commit_ms
    if not created:
        total_ms += select_ms

    logger.info("DB operations total: %.2f ms", total_ms)

    return transaction, created


async def get_transaction(
    db: AsyncSession,
    transaction_id: str,
) -> Transaction | None:
    """Retrieve a transaction by its ID."""

    return await db.get(Transaction, transaction_id)
