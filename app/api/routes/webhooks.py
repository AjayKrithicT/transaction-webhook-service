
import logging
from time import perf_counter

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.repositories.transaction_repository import (
    IdempotencyConflictError,
)
from app.schemas.transaction import (
    TransactionWebhookRequest,
    TransactionWebhookResponse,
)
from app.services.transaction_service import accept_transaction


# Configure INFO-level logging so timing messages can be displayed.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


router = APIRouter(
    prefix="/v1/webhooks",
    tags=["Webhooks"],
)


@router.post(
    "/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TransactionWebhookResponse,
)
async def receive_transaction_webhook(
    payload: TransactionWebhookRequest,
    db: AsyncSession = Depends(get_db),
) -> TransactionWebhookResponse:
    """Accept a transaction for asynchronous processing."""

    started_at = perf_counter()

    try:
        transaction_id, transaction_status, created = (
            await accept_transaction(db, payload)
        )

    except IdempotencyConflictError as exc:
        elapsed_ms = (perf_counter() - started_at) * 1000

        logger.info(
            "Webhook rejected due to idempotency conflict; "
            "handler time: %.2f ms",
            elapsed_ms,
        )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    elapsed_ms = (perf_counter() - started_at) * 1000

    logger.info(
        "Webhook handler completed in %.2f ms",
        elapsed_ms,
    )

    return TransactionWebhookResponse(
        transaction_id=transaction_id,
        status=transaction_status,
        message=(
            "Transaction accepted for processing"
            if created
            else "Transaction already received"
        ),
    )
