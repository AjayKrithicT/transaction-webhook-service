CREATE OR REPLACE FUNCTION public.accept_transaction_webhook(
    p_transaction_id VARCHAR(255),
    p_source_account VARCHAR(255),
    p_destination_account VARCHAR(255),
    p_amount NUMERIC(18, 2),
    p_currency CHAR(3)
)
RETURNS TABLE (
    result_transaction_id VARCHAR(255),
    result_status VARCHAR(20),
    result_created BOOLEAN,
    result_conflict BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_transaction public.transactions%ROWTYPE;
BEGIN
    INSERT INTO public.transactions (
        transaction_id,
        source_account,
        destination_account,
        amount,
        currency,
        status
    )
    VALUES (
        p_transaction_id,
        p_source_account,
        p_destination_account,
        p_amount,
        p_currency,
        'PROCESSING'
    )
    ON CONFLICT (transaction_id) DO NOTHING
    RETURNING * INTO v_transaction;

    IF FOUND THEN
        INSERT INTO public.processing_jobs (transaction_id, status)
        VALUES (v_transaction.transaction_id, 'PENDING');

        RETURN QUERY
        SELECT
            v_transaction.transaction_id,
            v_transaction.status,
            TRUE,
            FALSE;
        RETURN;
    END IF;

    SELECT t.*
    INTO v_transaction
    FROM public.transactions AS t
    WHERE t.transaction_id = p_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Transaction could not be retrieved after conflict';
    END IF;

    IF v_transaction.source_account IS DISTINCT FROM p_source_account
       OR v_transaction.destination_account IS DISTINCT FROM p_destination_account
       OR v_transaction.amount IS DISTINCT FROM p_amount
       OR v_transaction.currency IS DISTINCT FROM p_currency
    THEN
        RETURN QUERY
        SELECT
            v_transaction.transaction_id,
            v_transaction.status,
            FALSE,
            TRUE;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        v_transaction.transaction_id,
        v_transaction.status,
        FALSE,
        FALSE;
END;
$$;