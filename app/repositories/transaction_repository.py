from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus, TransactionStatus
from app.models.processing_job import ProcessingJob
from app.models.transaction import Transaction
from app.schemas.transaction import TransactionWebhookRequest


class IdempotencyConflictError(Exception):
    """Raised when a transaction ID is reused with a different payload."""


async def create_transaction(
    db: AsyncSession,
    payload: TransactionWebhookRequest,
) -> tuple[Transaction, bool]:
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

    result = await db.execute(
        select(Transaction).where(
            Transaction.transaction_id == payload.transaction_id
        )
    )
    transaction = result.scalar_one()

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

    await db.commit()

    return transaction, created


async def get_transaction(
    db: AsyncSession,
    transaction_id: str,
) -> Transaction | None:
    return await db.get(Transaction, transaction_id)
