# Transaction Webhook Service

A backend service built with **FastAPI, PostgreSQL, and SQLAlchemy** that accepts transaction webhooks, acknowledges requests quickly, and processes transactions asynchronously using a database-backed job queue.

The service supports idempotent transaction submission, persistent processing jobs, retry handling, and transaction status tracking.

## Features

- **Fast webhook acknowledgment:** Accepts valid transaction requests and responds with HTTP `202 Accepted` without waiting for the simulated external API call.
- **Asynchronous processing:** Processes accepted transactions in the background, simulating an external API call that takes approximately 30 seconds.
- **Idempotency:** Repeated requests with the same transaction ID and payload do not create duplicate transactions or processing jobs.
- **Conflict detection:** Returns HTTP `409 Conflict` when an existing transaction ID is submitted with a different payload.
- **Persistent job queue:** Stores processing jobs in PostgreSQL so pending work is not dependent solely on in-memory state.
- **Retry handling:** Retries failed jobs with exponential backoff and marks jobs as failed after the configured maximum number of attempts.
- **Concurrent worker coordination:** Uses PostgreSQL row locking with `FOR UPDATE SKIP LOCKED` and job leases to coordinate workers.
- **Transaction status tracking:** Exposes transaction details and processing status through a REST API.
- **Automated tests:** Covers API validation, idempotency, concurrent requests, processing, retries, and worker recovery.

## Tech Stack

- **Language:** Python
- **API framework:** FastAPI
- **Database:** PostgreSQL
- **ORM:** SQLAlchemy with asynchronous database access
- **Database driver:** asyncpg
- **Validation:** Pydantic
- **Testing:** pytest, FastAPI TestClient
- **Deployment:** Supabase for PostgreSQL and Render for API hosting

## Architecture

```text
Client
  |
  | POST /v1/webhooks/transactions
  v
FastAPI
  |
  | Validate request
  | Create transaction + processing job
  | Commit database transaction
  v
PostgreSQL
  |
  | HTTP 202 Accepted
  v
Client

Background Worker
  |
  | Claim pending job
  | Simulate external API call (~30 seconds)
  | Update transaction and job status
  v
PostgreSQL
  |
  v
GET /v1/transactions/{transaction_id}
```

The transaction and its processing job are created together in a database transaction. This ensures that an accepted transaction is persisted alongside the job required to process it.

The background worker claims available jobs using PostgreSQL row locking. Job leases and retry handling help recover work when processing fails or a worker stops unexpectedly.

The worker starts automatically through the FastAPI application lifecycle; a separate worker command is not required for the current implementation.

## Project Structure

```text
transaction-webhook-service/
├── app/
│   ├── api/
│   │   └── routes/
│   │       ├── health.py
│   │       ├── webhooks.py
│   │       └── transactions.py
│   ├── core/
│   │   ├── config.py
│   │   ├── constants.py
│   │   └── worker_config.py
│   ├── db/
│   │   ├── base.py
│   │   ├── session.py
│   │   ├── check_connection.py
│   │   └── setup_test_database.py
│   ├── models/
│   │   ├── transaction.py
│   │   └── processing_job.py
│   ├── repositories/
│   │   └── transaction_repository.py
│   ├── schemas/
│   │   └── transaction.py
│   ├── services/
│   │   └── transaction_service.py
│   ├── worker/
│   │   └── processor.py
│   └── main.py
├── database/
│   └── schema.sql
├── docs/
│   └── er-diagram.puml
├── tests/
│   ├── conftest.py
│   └── test_api.py
├── .env.example
├── .gitignore
├── pytest.ini
├── requirements.txt
└── README.md
```

## API Documentation

### 1. Health Check

**Endpoint:** `GET /`

Returns the API health status and current UTC timestamp.

Example response:

```json
{
  "status": "HEALTHY",
  "current_time": "2026-10-16T15:26:17.043989+00:00"
}
```

The timestamp in this example is illustrative.

### 2. Submit a Transaction Webhook

**Endpoint:** `POST /v1/webhooks/transactions`

Accepts a transaction and queues it for asynchronous processing.

Example request:

```json
{
  "transaction_id": "txn-001",
  "source_account": "ACC-1001",
  "destination_account": "ACC-2001",
  "amount": 125.50,
  "currency": "INR"
}
```

Example response:

```json
{
  "transaction_id": "txn-001",
  "status": "PROCESSING",
  "message": "Transaction accepted for processing"
}
```

The exact response message is determined by the API implementation.

**Expected HTTP status:** `202 Accepted`

The API persists the transaction and its processing job before returning the response. It does not wait for the simulated 30-second processing operation to finish.

Validation failures return `422 Unprocessable Entity`.

#### Idempotency behavior

Submitting the same transaction ID with the same payload returns the existing transaction without creating another processing job.

Submitting the same transaction ID with a different payload returns `409 Conflict`.

Example conflict response:

```json
{
  "detail": "Transaction ID already exists with a different payload."
}
```

### 3. Get Transaction Status

**Endpoint:** `GET /v1/transactions/{transaction_id}`

Returns the stored transaction details.

Example request:

```http
GET /v1/transactions/txn-001
```

Example response while processing:

```json
{
  "transaction_id": "txn-001",
  "source_account": "ACC-1001",
  "destination_account": "ACC-2001",
  "amount": 125.50,
  "currency": "INR",
  "status": "PROCESSING",
  "created_at": "2026-10-16T15:21:11.865178Z",
  "processed_at": null
}
```

Example response after successful processing:

```json
{
  "transaction_id": "txn-001",
  "source_account": "ACC-1001",
  "destination_account": "ACC-2001",
  "amount": 125.50,
  "currency": "INR",
  "status": "PROCESSED",
  "created_at": "2026-10-16T15:21:11.865178Z",
  "processed_at": "2026-10-16T15:21:47.182732Z"
}
```

The timestamps above are illustrative examples.

Possible transaction statuses:

| Status | Meaning |
|---|---|
| `PROCESSING` | The transaction is awaiting or undergoing processing. |
| `PROCESSED` | Processing completed successfully. |
| `FAILED` | Processing exhausted its configured retry attempts. |

`created_at` represents when the transaction was received and persisted. `processed_at` remains `null` until successful processing completes.

A transaction that does not exist returns `404 Not Found`.

## Database Design

The service uses two PostgreSQL tables.

### `transactions`

Stores the transaction payload and final processing state.

Important fields:

- `transaction_id`: Primary key.
- `source_account`: Source account identifier.
- `destination_account`: Destination account identifier.
- `amount`: Positive monetary amount with precision of 18 digits and scale of 2.
- `currency`: Three-character uppercase currency code.
- `status`: Transaction processing status.
- `received_at`: Transaction creation timestamp.
- `processed_at`: Successful processing timestamp, nullable until completion.

Database constraints enforce valid amounts, currency formats, status values, and timestamp consistency.

### `processing_jobs`

Stores the durable queue of background processing jobs.

Important fields:

- `transaction_id`: Unique foreign key to the transaction.
- `status`: Job state.
- `attempts` and `max_attempts`: Retry tracking.
- `available_at`: Earliest time the job can be claimed.
- `locked_until` and `locked_by`: Worker lease information.
- `last_error`: Details of the most recent processing failure.
- `created_at` and `updated_at`: Job timestamps.

Partial indexes help workers find pending jobs and identify expired processing leases.

The database schema is provided in `database/schema.sql`.

## Local Setup

### Prerequisites

Install the following:

- Python 3.11 or newer compatible with the project's dependencies.
- PostgreSQL.
- Git.

### 1. Clone the Repository

```bash
git clone https://github.com/AjayKrithicT/transaction-webhook-service.git
cd transaction-webhook-service
```

### 2. Create a Virtual Environment

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

On macOS or Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Create PostgreSQL Databases

Create separate databases for application data and automated tests:

```sql
CREATE DATABASE transactions_db;
CREATE DATABASE transactions_test_db;
```

Run these statements using a PostgreSQL account with permission to create databases.

### 5. Configure Environment Variables

Copy the example configuration.

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS or Linux:

```bash
cp .env.example .env
```

Edit `.env` and set the database connection strings for your environment.

```dotenv
DATABASE_URL=postgresql+asyncpg://postgres:YOUR_PASSWORD@localhost:5432/transactions_db
TEST_DATABASE_URL=postgresql+asyncpg://postgres:YOUR_PASSWORD@localhost:5432/transactions_test_db
```

Replace `YOUR_PASSWORD` with your local PostgreSQL password.

If your password contains reserved URL characters, percent-encode them in the connection URL.

**Security:** Never commit `.env` or publish actual database credentials. `.env.example` must contain placeholders only.

### 6. Initialize the Application Database

Apply the SQL schema to the application database:

```bash
psql -d "postgresql://USER:PASSWORD@HOST:5432/transactions_db" -f database/schema.sql
```

Replace the connection details with those of your application database.

The application does not automatically create the production schema at startup. Initialize the schema before starting the service.

For local PostgreSQL, you can also use the appropriate local connection string.

### 7. Verify the Database Connection

Activate the virtual environment, then run:

```bash
python -m app.db.check_connection
```

The command should confirm that the database connection succeeds.

### 8. Start the API

```bash
python -m uvicorn app.main:app --reload
```

The API is available at:

- Health check: `http://127.0.0.1:8000/`
- Interactive API documentation: `http://127.0.0.1:8000/docs`

The background worker starts with the application.

## Testing

Automated tests use the separate `TEST_DATABASE_URL` database to avoid modifying application data.

Ensure `TEST_DATABASE_URL` points to `transactions_test_db`, not the application or production database.

Run the test suite:

```bash
python -m pytest -v
```

The suite currently contains 17 tests covering:

- Health endpoint behavior.
- Request validation.
- Missing transaction handling.
- Duplicate and concurrent duplicate submissions.
- Fast webhook acknowledgment.
- Transaction status responses.
- Successful background processing.
- Job claiming and retry scheduling.
- Failure after maximum attempts.
- Recovery of expired leases.
- Conflicting payloads.
- Protection against stale workers completing reclaimed jobs.

**Latest local test result:** 17 passed in 3.60 seconds.

A Starlette deprecation warning was also observed concerning the use of `httpx` with `starlette.testclient`. It did not cause test failures.

## Deployment

The intended hosting setup uses Supabase for PostgreSQL and Render for the FastAPI service.

### Supabase

1. Create a PostgreSQL project.
2. Initialize the database using `database/schema.sql` in the SQL Editor.
3. Obtain a PostgreSQL connection string compatible with the hosting environment.
4. Configure the application's `DATABASE_URL` using the SQLAlchemy async driver format.

Example format:

```dotenv
DATABASE_URL=postgresql+asyncpg://DB_USER:DB_PASSWORD@DB_HOST:5432/postgres
```

Use your actual host, username, and password in the hosting environment, not in this README.

The deployed application must use the initialized database schema.

### Render

1. Create a new Render Web Service connected to the GitHub repository.
2. Select the appropriate Python runtime.
3. Set the build command:

   ```bash
   pip install -r requirements.txt
   ```

4. Set the start command:

   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port $PORT
   ```

5. Add `DATABASE_URL` as a Render environment variable, using the Supabase connection details.
6. Do not configure the local `TEST_DATABASE_URL` as a production database setting.
7. Deploy the service and wait for the deployment to become healthy.

After deployment, verify the health endpoint, webhook submission, transaction status endpoint, duplicate behavior, and background processing using the public Render URL.

Keep database credentials in the hosting provider's environment-variable settings. Never put real credentials in source code, Git commits, screenshots, or public issue reports.

## Hosting Limitations

This project uses free-tier hosting for demonstration and assessment purposes. Free-tier behavior can affect availability and observed response times.

- **Render cold starts:** Free web services may spin down after a period of inactivity. The first request after spin-down can experience a cold start and take longer than usual to respond.
- **Supabase inactivity:** Free-tier database projects may be paused after extended inactivity. Database operations can be unavailable until the project resumes.
- **Latency:** The webhook is designed to acknowledge accepted requests quickly, while transaction processing runs asynchronously and simulates an external API call lasting approximately 30 seconds. Cold starts, database availability, network latency, and resource limits can affect observed timings.
- **Availability:** These free-tier services are appropriate for demonstrations and assessments, but they do not provide the same availability guarantees as a suitably configured production environment.
- **Production readiness:** Production use should include appropriate hosting capacity, monitoring, alerting, secure secret management, database backups, and operational recovery procedures.

A successful local test does not guarantee identical cold-start behavior or latency on the hosted service.

## Design Considerations

### Why use a database-backed queue?

An in-memory background task can be lost if the application process stops. Persisting jobs in PostgreSQL allows pending work to be recovered after restarts and provides durable processing state.

### Why use idempotency?

Webhook providers may retry requests when acknowledgments are delayed or lost. Idempotency prevents repeated delivery of the same transaction from creating duplicate processing work.

### Why use worker leases?

A lease allows another worker to reclaim a job if the original worker stops making progress. Lease checks also help prevent an outdated worker from completing a job after another worker has reclaimed it.

### Why separate transactions and jobs?

Transaction data represents the business operation, while the job record represents the work required to process it. Keeping these concepts separate makes retries, job claiming, and processing state easier to manage.

## Future Improvements

Potential improvements for a production deployment include:

- Authentication and webhook signature verification.
- Structured logging, metrics, and distributed tracing.
- A managed queue such as Redis-backed workers or a cloud messaging service.
- Database migration management.
- Rate limiting and request-size limits.
- Dead-letter handling and operational replay tools.
- Integration tests against the actual deployed environment.
- Monitoring and alerting for stuck jobs and processing latency.

## License

Add a license file if this repository is intended for public reuse. Until a license is included, all rights remain with the copyright holder by default.
