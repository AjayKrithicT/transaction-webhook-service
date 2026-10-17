from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.transaction_repository import (
    IdempotencyConflictError,
    create_transaction,
    get_transaction,
)
from app.schemas.transaction import TransactionWebhookRequest


async def accept_transaction(
    db: AsyncSession,
    payload: TransactionWebhookRequest,
) -> tuple[str, str, bool]:
    """
    Accept a transaction and ensure its background job is persisted.

    Returns:
        transaction_id, status, created

    Raises:
        IdempotencyConflictError: If the transaction ID already exists
        with a different payload.
    """
    transaction, created = await create_transaction(db, payload)

    return transaction.transaction_id, transaction.status, created


async def fetch_transaction_status(
    db: AsyncSession,
    transaction_id: str,
):
    """Retrieve a transaction by its ID."""

    return await get_transaction(db, transaction_id)
