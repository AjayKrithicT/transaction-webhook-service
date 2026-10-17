from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.transaction import TransactionStatusResponse
from app.services.transaction_service import fetch_transaction_status


router = APIRouter(prefix="/v1/transactions", tags=["Transactions"])


@router.get(
    "/{transaction_id}",
    response_model=TransactionStatusResponse,
)
async def get_transaction_status(
    transaction_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get the current processing status of a transaction."""

    transaction = await fetch_transaction_status(db, transaction_id)

    if transaction is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Transaction not found",
        )

    return transaction
