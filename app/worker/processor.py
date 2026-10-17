import asyncio
import logging
import socket
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus, TransactionStatus
from app.core.worker_config import (
    JOB_LEASE_SECONDS,
    POLL_INTERVAL_SECONDS,
    PROCESSING_DELAY_SECONDS,
    RETRY_BASE_DELAY_SECONDS,
)
from app.db.session import AsyncSessionLocal
from app.models.processing_job import ProcessingJob
from app.models.transaction import Transaction


logger = logging.getLogger(__name__)

# Tests can replace this factory with one connected to the test database.
SessionLocal = AsyncSessionLocal

# Simulated external processing delay. Tests can override it.
PROCESSING_DELAY = PROCESSING_DELAY_SECONDS

# Each running worker process gets a unique identity.
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4()}"


def utc_now() -> datetime:
    """Return the current timezone-aware UTC time."""
    return datetime.now(timezone.utc)


async def claim_job(db: AsyncSession) -> ProcessingJob | None:
    """Atomically claim one available job."""

    now = utc_now()

    statement = (
        select(ProcessingJob)
        .where(
            ProcessingJob.attempts < ProcessingJob.max_attempts,
            or_(
                (
                    (ProcessingJob.status == JobStatus.PENDING.value)
                    & (ProcessingJob.available_at <= now)
                ),
                (
                    (ProcessingJob.status == JobStatus.PROCESSING.value)
                    & (ProcessingJob.locked_until <= now)
                ),
            ),
        )
        .order_by(ProcessingJob.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )

    try:
        result = await db.execute(statement)
        job = result.scalar_one_or_none()

        if job is None:
            await db.rollback()
            return None

        job.status = JobStatus.PROCESSING.value
        job.attempts += 1
        job.locked_by = WORKER_ID
        job.locked_until = now + timedelta(
            seconds=JOB_LEASE_SECONDS
        )
        job.updated_at = now

        await db.commit()

        logger.info(
            "Claimed job %s for transaction %s (attempt %s/%s)",
            job.id,
            job.transaction_id,
            job.attempts,
            job.max_attempts,
        )

        return job

    except Exception:
        await db.rollback()
        raise


async def recover_exhausted_jobs() -> None:
    """Fail expired processing jobs that have exhausted all attempts."""

    async with SessionLocal() as db:
        try:
            now = utc_now()

            statement = (
                select(ProcessingJob)
                .where(
                    ProcessingJob.status == JobStatus.PROCESSING.value,
                    ProcessingJob.locked_until <= now,
                    ProcessingJob.attempts >= ProcessingJob.max_attempts,
                )
                .with_for_update(skip_locked=True)
            )

            result = await db.execute(statement)
            jobs = result.scalars().all()

            for job in jobs:
                transaction_result = await db.execute(
                    select(Transaction).where(
                        Transaction.transaction_id
                        == job.transaction_id
                    )
                )
                transaction = transaction_result.scalar_one_or_none()

                if transaction is not None:
                    transaction.status = TransactionStatus.FAILED.value
                    transaction.processed_at = None

                job.status = JobStatus.FAILED.value
                job.last_error = (
                    "Processing lease expired after maximum attempts"
                )
                job.locked_by = None
                job.locked_until = None
                job.updated_at = now

                logger.error(
                    "Job %s failed after lease expiry",
                    job.id,
                )

            await db.commit()

        except Exception:
            await db.rollback()
            raise


async def handle_processing_failure(
    job_id: int,
    transaction_id: str,
    error_message: str,
) -> None:
    """Schedule a retry or permanently fail a processing job."""

    async with SessionLocal() as db:
        try:
            result = await db.execute(
                select(ProcessingJob)
                .where(
                    ProcessingJob.id == job_id,
                    ProcessingJob.status == JobStatus.PROCESSING.value,
                    ProcessingJob.locked_by == WORKER_ID,
                )
                .with_for_update()
            )

            job = result.scalar_one_or_none()

            if job is None:
                logger.warning(
                    "Job %s is no longer owned by this worker",
                    job_id,
                )
                return

            now = utc_now()

            result = await db.execute(
                select(Transaction).where(
                    Transaction.transaction_id == transaction_id
                )
            )
            transaction = result.scalar_one_or_none()

            job.last_error = error_message[:4000]
            job.locked_by = None
            job.locked_until = None
            job.updated_at = now

            if job.attempts < job.max_attempts:
                # Exponential backoff: base, 2x base, 4x base, etc.
                delay_seconds = RETRY_BASE_DELAY_SECONDS * (
                    2 ** (job.attempts - 1)
                )

                job.status = JobStatus.PENDING.value
                job.available_at = now + timedelta(
                    seconds=delay_seconds
                )

                logger.warning(
                    "Job %s scheduled for retry in %s seconds",
                    job.id,
                    delay_seconds,
                )

            else:
                job.status = JobStatus.FAILED.value

                if transaction is not None:
                    transaction.status = TransactionStatus.FAILED.value
                    transaction.processed_at = None

                logger.error(
                    "Job %s permanently failed after %s attempts",
                    job.id,
                    job.attempts,
                )

            await db.commit()

        except Exception:
            await db.rollback()
            logger.exception(
                "Could not record failure for job %s",
                job_id,
            )
            raise


async def process_job(job: ProcessingJob) -> None:
    """Simulate external processing and persist the final result."""

    logger.info(
        "Processing transaction %s",
        job.transaction_id,
    )

    try:
        # Simulate an external API taking 30 seconds.
        await asyncio.sleep(PROCESSING_DELAY)

        async with SessionLocal() as db:
            try:
                # Verify ownership and lease validity before completing.
                result = await db.execute(
                    select(ProcessingJob)
                    .where(
                        ProcessingJob.id == job.id,
                        ProcessingJob.status == JobStatus.PROCESSING.value,
                        ProcessingJob.locked_by == WORKER_ID,
                        ProcessingJob.locked_until > utc_now(),
                    )
                    .with_for_update()
                )

                current_job = result.scalar_one_or_none()

                if current_job is None:
                    logger.warning(
                        "Job %s is no longer owned by this worker "
                        "or its lease has expired",
                        job.id,
                    )
                    return

                result = await db.execute(
                    select(Transaction).where(
                        Transaction.transaction_id
                        == job.transaction_id
                    )
                )
                transaction = result.scalar_one_or_none()

                if transaction is None:
                    raise ValueError(
                        f"Transaction {job.transaction_id} "
                        "was not found"
                    )

                now = utc_now()

                transaction.status = TransactionStatus.PROCESSED.value
                transaction.processed_at = now

                current_job.status = JobStatus.COMPLETED.value
                current_job.locked_by = None
                current_job.locked_until = None
                current_job.last_error = None
                current_job.updated_at = now

                await db.commit()

                logger.info(
                    "Transaction %s processed successfully",
                    job.transaction_id,
                )

            except Exception:
                await db.rollback()
                raise

    except asyncio.CancelledError:
        # Allow the worker task to shut down cleanly.
        raise

    except Exception as exc:
        logger.exception(
            "Processing failed for transaction %s",
            job.transaction_id,
        )

        await handle_processing_failure(
            job.id,
            job.transaction_id,
            str(exc) or type(exc).__name__,
        )


async def worker_loop() -> None:
    """Continuously poll PostgreSQL for available processing jobs."""

    logger.info(
        "Background worker started: %s",
        WORKER_ID,
    )

    while True:
        try:
            await recover_exhausted_jobs()

            async with SessionLocal() as db:
                job = await claim_job(db)

            if job is None:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            await process_job(job)

        except asyncio.CancelledError:
            logger.info("Background worker shutting down")
            raise

        except Exception:
            logger.exception("Unexpected worker error")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    asyncio.run(worker_loop())
