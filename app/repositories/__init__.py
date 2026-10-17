from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transaction import Transaction
from app.models.processing_job import ProcessingJob
from app.schemas.transaction import TransactionWebhookRequest


async def create_transaction(
    db: AsyncSession,
    payload: TransactionWebhookRequest,
) -> tuple[Transaction, bool]:

    async with db.begin():
        # Insert only if this transaction ID does not already exist.
        statement = (
            insert(Transaction)
            .values(
                transaction_id=payload.transaction_id,
                source_account=payload.source_account,
                destination_account=payload.destination_account,
                amount=payload.amount,
                currency=payload.currency,
                status="PROCESSING",
            )
            .on_conflict_do_nothing(
                index_elements=[Transaction.transaction_id]
            )
            .returning(Transaction.transaction_id)
        )

        result = await db.execute(statement)
        created_id = result.scalar_one_or_none()

        if created_id is not None:
            # Create a job only when the transaction was inserted.
            db.add(
                ProcessingJob(
                    transaction_id=created_id,
                    status="PENDING",
                )
            )
            created = True
        else:
            # Another request already created this transaction.
            created = False

        # Load the transaction within this database transaction.
        result = await db.execute(
            select(Transaction).where(
                Transaction.transaction_id == payload.transaction_id
            )
        )
        transaction = result.scalar_one()

    return transaction, created


async def get_transaction(
    db: AsyncSession,
    transaction_id: str,
) -> Transaction | None:
    return await db.get(Transaction, transaction_id)
