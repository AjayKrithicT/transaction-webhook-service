import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.core.constants import JobStatus, TransactionStatus
from app.main import app
from app.models.processing_job import ProcessingJob
from app.models.transaction import Transaction
from app.worker import processor
from tests.conftest import TestSessionLocal

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/")

    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "HEALTHY"
    assert "current_time" in data


def test_webhook_rejects_negative_amount():
    payload = {
        "transaction_id": "negative-amount-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "-100.00",
        "currency": "USD",
    }

    response = client.post("/v1/webhooks/transactions", json=payload)

    assert response.status_code == 422


def test_webhook_rejects_invalid_currency():
    payload = {
        "transaction_id": "invalid-currency-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "US",
    }

    response = client.post("/v1/webhooks/transactions", json=payload)

    assert response.status_code == 422


def test_missing_transaction_returns_404():
    response = client.get(
        "/v1/transactions/transaction-that-does-not-exist"
    )

    assert response.status_code == 404


def test_duplicate_webhook_creates_only_one_transaction_and_job():
    payload = {
        "transaction_id": "duplicate-test-001",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    first_response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    second_response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    assert (
        first_response.json()["message"]
        == "Transaction accepted for processing"
    )
    assert (
        second_response.json()["message"]
        == "Transaction already received"
    )

    async def verify():
        async with TestSessionLocal() as session:
            transaction_count = await session.scalar(
                select(func.count())
                .select_from(Transaction)
                .where(
                    Transaction.transaction_id
                    == payload["transaction_id"]
                )
            )

            job_count = await session.scalar(
                select(func.count())
                .select_from(ProcessingJob)
                .where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )

            assert transaction_count == 1
            assert job_count == 1

    asyncio.run(verify())


def test_concurrent_duplicate_webhooks_create_only_one_transaction_and_job():
    payload = {
        "transaction_id": "concurrent-duplicate-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    def send_request():
        return client.post(
            "/v1/webhooks/transactions",
            json=payload,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(
            executor.map(lambda _: send_request(), range(2))
        )

    assert all(response.status_code == 202 for response in responses)

    async def verify():
        async with TestSessionLocal() as session:
            transaction_count = await session.scalar(
                select(func.count())
                .select_from(Transaction)
                .where(
                    Transaction.transaction_id
                    == payload["transaction_id"]
                )
            )

            job_count = await session.scalar(
                select(func.count())
                .select_from(ProcessingJob)
                .where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )

            assert transaction_count == 1
            assert job_count == 1

    asyncio.run(verify())


def test_webhook_returns_quickly_and_starts_processing():
    payload = {
        "transaction_id": "quick-response-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    start_time = time.perf_counter()

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )

    elapsed = time.perf_counter() - start_time

    assert response.status_code == 202
    assert elapsed < 0.5

    data = response.json()
    assert data["transaction_id"] == payload["transaction_id"]
    assert data["status"] == TransactionStatus.PROCESSING.value

    status_response = client.get(
        f"/v1/transactions/{payload['transaction_id']}"
    )

    assert status_response.status_code == 200
    assert status_response.json()["status"] == (
        TransactionStatus.PROCESSING.value
    )
    assert status_response.json()["processed_at"] is None

def test_transaction_status_returns_assignment_fields():
    payload = {
        "transaction_id": "status-contract-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "1500.50",
        "currency": "INR",
    }

    create_response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert create_response.status_code == 202

    response = client.get(
        f"/v1/transactions/{payload['transaction_id']}"
    )

    assert response.status_code == 200

    data = response.json()

    # The endpoint returns one JSON object, not an array.
    assert isinstance(data, dict)

    # Verify every field expected by the assignment.
    assert set(data.keys()) == {
        "transaction_id",
        "source_account",
        "destination_account",
        "amount",
        "currency",
        "status",
        "created_at",
        "processed_at",
    }

    assert data["transaction_id"] == payload["transaction_id"]
    assert data["source_account"] == payload["source_account"]
    assert data["destination_account"] == payload["destination_account"]
    assert float(data["amount"]) == 1500.50
    assert data["currency"] == "INR"
    assert data["status"] == TransactionStatus.PROCESSING.value
    assert data["created_at"] is not None
    assert data["processed_at"] is None

def test_worker_processes_transaction_successfully(monkeypatch):
    payload = {
        "transaction_id": "worker-success-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    monkeypatch.setattr(processor, "PROCESSING_DELAY", 0)

    async def run_test():
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            claimed_job = await processor.claim_job(session)
            assert claimed_job is not None

            job_id = claimed_job.id
            transaction_id = claimed_job.transaction_id

        await processor.process_job(claimed_job)

        async with TestSessionLocal() as session:
            transaction = await session.get(
                Transaction,
                transaction_id,
            )
            job = await session.get(ProcessingJob, job_id)

            assert transaction.status == TransactionStatus.PROCESSED.value
            assert transaction.processed_at is not None
            assert job.status == JobStatus.COMPLETED.value
            assert job.locked_until is None
            assert job.locked_by is None

    asyncio.run(run_test())


def test_worker_claims_pending_job():
    payload = {
        "transaction_id": "worker-claim-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    async def run_test():
        async with TestSessionLocal() as session:
            job = await processor.claim_job(session)

            assert job is not None
            assert job.transaction_id == payload["transaction_id"]
            assert job.status == JobStatus.PROCESSING.value
            assert job.attempts == 1
            assert job.locked_by is not None
            assert job.locked_until is not None

    asyncio.run(run_test())


def test_worker_schedules_retry_after_failure():
    payload = {
        "transaction_id": "worker-retry-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    async def run_test():
        async with TestSessionLocal() as session:
            job = await processor.claim_job(session)
            assert job is not None

            job_id = job.id
            transaction_id = job.transaction_id
            attempts = job.attempts

        await processor.handle_processing_failure(
            job_id,
            transaction_id,
            "Simulated temporary failure",
        )

        async with TestSessionLocal() as session:
            job = await session.get(ProcessingJob, job_id)

            assert job.status == JobStatus.PENDING.value
            assert job.attempts == attempts
            assert job.last_error == "Simulated temporary failure"
            assert job.available_at > datetime.now(timezone.utc)
            assert job.locked_until is None
            assert job.locked_by is None

    asyncio.run(run_test())


def test_worker_marks_job_failed_after_max_attempts():
    payload = {
        "transaction_id": "worker-final-failure-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    async def run_test():
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            job.status = JobStatus.PROCESSING.value
            job.attempts = job.max_attempts
            job.locked_by = processor.WORKER_ID
            job.locked_until = datetime.now(timezone.utc) + timedelta(
                seconds=60
            )
            await session.commit()

            job_id = job.id
            transaction_id = job.transaction_id

        await processor.handle_processing_failure(
            job_id,
            transaction_id,
            "Simulated permanent failure",
        )

        async with TestSessionLocal() as session:
            job = await session.get(ProcessingJob, job_id)
            transaction = await session.get(
                Transaction,
                transaction_id,
            )

            assert job.status == JobStatus.FAILED.value
            assert job.last_error == "Simulated permanent failure"
            assert job.locked_until is None
            assert job.locked_by is None
            assert transaction.status == TransactionStatus.FAILED.value
            assert transaction.processed_at is None

    asyncio.run(run_test())


def test_worker_reclaims_job_after_lease_expires():
    payload = {
        "transaction_id": "worker-expired-lease-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    async def run_test():
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            job.status = JobStatus.PROCESSING.value
            job.locked_by = "old-worker"
            job.locked_until = datetime.now(timezone.utc) - timedelta(
                seconds=10
            )
            await session.commit()

        async with TestSessionLocal() as session:
            reclaimed_job = await processor.claim_job(session)

            assert reclaimed_job is not None
            assert reclaimed_job.transaction_id == payload["transaction_id"]
            assert reclaimed_job.status == JobStatus.PROCESSING.value
            assert reclaimed_job.attempts == 1
            assert reclaimed_job.locked_by == processor.WORKER_ID
            assert reclaimed_job.locked_until > datetime.now(timezone.utc)

    asyncio.run(run_test())


def test_conflicting_duplicate_payload_returns_409():
    original_payload = {
        "transaction_id": "idempotency-conflict-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    conflicting_payloads = [
        {**original_payload, "amount": "200.00"},
        {**original_payload, "currency": "EUR"},
        {**original_payload, "source_account": "ACCOUNT-003"},
        {**original_payload, "destination_account": "ACCOUNT-004"},
    ]

    original_response = client.post(
        "/v1/webhooks/transactions",
        json=original_payload,
    )
    assert original_response.status_code == 202

    for conflicting_payload in conflicting_payloads:
        response = client.post(
            "/v1/webhooks/transactions",
            json=conflicting_payload,
        )

        assert response.status_code == 409
        assert response.json()["detail"] == (
            "Transaction ID already exists with a different payload."
        )

    async def verify_original_transaction():
        async with TestSessionLocal() as session:
            transaction = await session.get(
                Transaction,
                original_payload["transaction_id"],
            )

            assert transaction is not None
            assert transaction.source_account == "ACCOUNT-001"
            assert transaction.destination_account == "ACCOUNT-002"
            assert transaction.amount == Decimal("100.00")
            assert transaction.currency == "USD"

            job_count = await session.scalar(
                select(func.count())
                .select_from(ProcessingJob)
                .where(
                    ProcessingJob.transaction_id
                    == original_payload["transaction_id"]
                )
            )

            assert job_count == 1

    asyncio.run(verify_original_transaction())


def test_worker_recovers_exhausted_expired_jobs():
    payload = {
        "transaction_id": "worker-recovery-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    async def run_test():
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            # Simulate a crashed worker that exhausted all attempts.
            job.status = JobStatus.PROCESSING.value
            job.attempts = job.max_attempts
            job.locked_by = "crashed-worker"
            job.locked_until = datetime.now(timezone.utc) - timedelta(
                seconds=10
            )
            await session.commit()

            job_id = job.id
            transaction_id = job.transaction_id

        await processor.recover_exhausted_jobs()

        async with TestSessionLocal() as session:
            job = await session.get(ProcessingJob, job_id)
            transaction = await session.get(
                Transaction,
                transaction_id,
            )

            assert job.status == JobStatus.FAILED.value
            assert job.last_error == (
                "Processing lease expired after maximum attempts"
            )
            assert job.locked_by is None
            assert job.locked_until is None

            assert transaction.status == TransactionStatus.FAILED.value
            assert transaction.processed_at is None

    asyncio.run(run_test())


def test_worker_does_not_complete_job_after_lease_expires(monkeypatch):
    payload = {
        "transaction_id": "lease-expiry-completion-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    # Skip the simulated external processing delay.
    monkeypatch.setattr(processor, "PROCESSING_DELAY", 0)

    async def run_test():
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            # Simulate a job claimed by this worker whose lease has expired.
            job.status = JobStatus.PROCESSING.value
            job.attempts = 1
            job.locked_by = processor.WORKER_ID
            job.locked_until = datetime.now(timezone.utc) - timedelta(
                seconds=1
            )
            await session.commit()

            job_id = job.id
            transaction_id = job.transaction_id

        await processor.process_job(job)

        async with TestSessionLocal() as session:
            transaction = await session.get(
                Transaction,
                transaction_id,
            )
            saved_job = await session.get(ProcessingJob, job_id)

            assert transaction is not None
            assert transaction.status == TransactionStatus.PROCESSING.value
            assert transaction.processed_at is None

            assert saved_job is not None
            assert saved_job.status == JobStatus.PROCESSING.value
            assert saved_job.locked_by == processor.WORKER_ID

    asyncio.run(run_test())

def test_old_worker_cannot_complete_job_after_reclaim(monkeypatch):
    payload = {
        "transaction_id": "worker-reclaim-race-test",
        "source_account": "ACCOUNT-001",
        "destination_account": "ACCOUNT-002",
        "amount": "100.00",
        "currency": "USD",
    }

    response = client.post(
        "/v1/webhooks/transactions",
        json=payload,
    )
    assert response.status_code == 202

    # Prevent the test from waiting for the simulated processing delay.
    monkeypatch.setattr(processor, "PROCESSING_DELAY", 0)

    async def run_test():
        # Step 1: Simulate the original worker claiming the job.
        async with TestSessionLocal() as session:
            job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert job is not None

            job.status = JobStatus.PROCESSING.value
            job.attempts = 1
            job.locked_by = processor.WORKER_ID
            job.locked_until = datetime.now(timezone.utc) + timedelta(
                seconds=60
            )

            await session.commit()
            await session.refresh(job)

            # Keep this ORM object as the original worker's job reference.
            original_worker_job = job

        # Step 2: Simulate another worker reclaiming the job.
        async with TestSessionLocal() as session:
            reclaimed_job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )
            assert reclaimed_job is not None

            reclaimed_job.attempts = 2
            reclaimed_job.locked_by = "replacement-worker"
            reclaimed_job.locked_until = (
                datetime.now(timezone.utc) + timedelta(seconds=60)
            )

            await session.commit()

        # Step 3: The original worker attempts to complete its stale job.
        await processor.process_job(original_worker_job)

        # Step 4: Verify the original worker did not overwrite the job.
        async with TestSessionLocal() as session:
            transaction = await session.get(
                Transaction,
                payload["transaction_id"],
            )

            saved_job = await session.scalar(
                select(ProcessingJob).where(
                    ProcessingJob.transaction_id
                    == payload["transaction_id"]
                )
            )

            assert transaction is not None
            assert transaction.status == TransactionStatus.PROCESSING.value
            assert transaction.processed_at is None

            assert saved_job is not None
            assert saved_job.status == JobStatus.PROCESSING.value
            assert saved_job.attempts == 2
            assert saved_job.locked_by == "replacement-worker"
            assert saved_job.locked_until is not None

    asyncio.run(run_test())
