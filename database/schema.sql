
CREATE TABLE transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    source_account VARCHAR(255) NOT NULL,
    destination_account VARCHAR(255) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL
    CHECK (currency ~ '^[A-Z]{3}$'),
    status VARCHAR(20) NOT NULL DEFAULT 'PROCESSING'
        CHECK (status IN ('PROCESSING', 'PROCESSED', 'FAILED')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,

    CHECK (
        (status = 'PROCESSED' AND processed_at IS NOT NULL)
        OR
        (status IN ('PROCESSING', 'FAILED') AND processed_at IS NULL)
    )
);

CREATE TABLE processing_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL UNIQUE
        REFERENCES transactions(transaction_id),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ NULL,
    locked_by VARCHAR(255) NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processing_jobs_pending
    ON processing_jobs (available_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_processing_jobs_expired_leases
    ON processing_jobs (locked_until)
    WHERE status = 'PROCESSING';
