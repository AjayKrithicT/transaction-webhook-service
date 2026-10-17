
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

    # 1. Measure the INSERT operation.
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
        .returning(Transaction.transaction_id)
    )

    result = await db.execute(statement)
    created_id = result.scalar_one_or_none()

    insert_ms = (perf_counter() - insert_started) * 1000

    logger.info(
        "DB INSERT duration: %.2f ms",
        insert_ms,
    )

    if created_id is not None:
        # Only a newly created transaction gets a processing job.
        db.add(
            ProcessingJob(
                transaction_id=created_id,
                status=JobStatus.PENDING.value,
            )
        )
        created = True
    else:
        created = False

    # 2. Measure the transaction lookup.
    select_started = perf_counter()

    result = await db.execute(
        select(Transaction).where(
            Transaction.transaction_id == payload.transaction_id
        )
    )
    transaction = result.scalar_one()

    select_ms = (perf_counter() - select_started) * 1000

    logger.info(
        "DB SELECT duration: %.2f ms",
        select_ms,
    )

    # Reusing an ID is allowed only when all original fields match.
    payload_matches = (
        transaction.source_account == payload.source_account
        and transaction.destination_account == payload.destination_account
        and transaction.amount == payload.amount
        and transaction.currency == payload.currency
    )

    if not payload_matches:
        await db.rollback()

        raise IdempotencyConflictError(
            "Transaction ID already exists with a different payload."
        )

    # 3. Measure the commit, which also persists a newly added job.
    commit_started = perf_counter()

    await db.commit()

    commit_ms = (perf_counter() - commit_started) * 1000

    logger.info(
        "DB COMMIT duration: %.2f ms",
        commit_ms,
    )

    logger.info(
        "DB operations total: %.2f ms",
        insert_ms + select_ms + commit_ms,
    )

    return transaction, created


async def get_transaction(
    db: AsyncSession,
    transaction_id: str,
) -> Transaction | None:
    """Retrieve a transaction by its ID."""

    return await db.get(Transaction, transaction_id)
