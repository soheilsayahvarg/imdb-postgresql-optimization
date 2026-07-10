-- =============================================================================
--  Message queue functions (PL/pgSQL)
--
--  Apply with:
--      docker exec -i imdb_postgres psql -U imdb -d imdb -v ON_ERROR_STOP=1 \
--          < sql/01_queue_functions.sql
--
--  Delivery semantics: AT-LEAST-ONCE, not exactly-once.
--  A consumer can crash after inserting rows but before calling ack(). The lease
--  then expires, reap_expired() returns the message to 'pending', and another
--  consumer processes it a second time. There is no fencing token, so a late
--  ack() from the first consumer cannot be distinguished from a legitimate one.
--  This is inherent to a table-as-a-queue built on SKIP LOCKED, and it is exactly
--  why the ingestion path must be idempotent -- hence ON CONFLICT DO NOTHING on
--  every target table.
--
--  State machine:
--
--      enqueue()                 dequeue()                 ack()
--      -------->  pending  ---------------->  processing  -------->  done
--                    ^                            |
--                    |   nack()  (attempts < max) |
--                    +----------------------------+
--                    |   reap_expired()           |
--                    |   (lease expired)          |
--                    |                            |
--                    |   nack() / reap_expired()  v
--                    +--- (attempts >= max) --->  failed
--
--  `attempts` counts FAILED deliveries: it is incremented by nack() and by
--  reap_expired(), never by dequeue(). A message that is claimed and acked on
--  the first try ends with attempts = 0.
-- =============================================================================

\set ON_ERROR_STOP on

SET search_path TO imdb, public;

-- -----------------------------------------------------------------------------
--  Supporting index for purge_done().
--
--  Without it, purging a drained queue seq-scans the whole table on every call.
--  Like message_queue_ready_idx, it is partial, so it only ever holds the rows
--  that are actually waiting to be reclaimed.
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS message_queue_done_idx
    ON imdb.message_queue (queue_name, id)
    WHERE status = 'done';


-- =============================================================================
--  1. enqueue -- insert a message into the queue
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.enqueue(
    p_queue_name TEXT,
    p_payload    JSONB
) RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_id BIGINT;
BEGIN
    IF p_queue_name IS NULL OR btrim(p_queue_name) = '' THEN
        RAISE EXCEPTION 'enqueue: queue_name must be a non-empty string';
    END IF;

    -- The column is NOT NULL, but a named error beats a constraint violation.
    IF p_payload IS NULL THEN
        RAISE EXCEPTION 'enqueue: payload must not be NULL (queue=%)', p_queue_name;
    END IF;

    INSERT INTO imdb.message_queue (queue_name, payload)
    VALUES (p_queue_name, p_payload)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

COMMENT ON FUNCTION imdb.enqueue(TEXT, JSONB) IS
    'Append one message. Returns its id. Payload is {"table","data"} or {"table","rows":[...]}';


-- =============================================================================
--  1b. enqueue_many -- bulk variant, one round trip for N messages
--
--  The producer sends ~190k batch messages. Doing that one statement at a time
--  is not slow because of PostgreSQL, it is slow because of network round trips.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.enqueue_many(
    p_queue_name TEXT,
    p_payloads   JSONB[]
) RETURNS SETOF BIGINT
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    IF p_queue_name IS NULL OR btrim(p_queue_name) = '' THEN
        RAISE EXCEPTION 'enqueue_many: queue_name must be a non-empty string';
    END IF;

    -- The INSERT is wrapped in a CTE so that RETURN QUERY is handed a plain
    -- SELECT. `RETURN QUERY <dml> RETURNING ...` also works, but this form is
    -- unambiguous and lets the result be ordered.
    RETURN QUERY
    WITH inserted AS (
        INSERT INTO imdb.message_queue (queue_name, payload)
        SELECT p_queue_name, p.payload
        FROM   unnest(p_payloads) AS p(payload)
        WHERE  p.payload IS NOT NULL
        RETURNING id
    )
    SELECT i.id FROM inserted i ORDER BY i.id;
END;
$$;

COMMENT ON FUNCTION imdb.enqueue_many(TEXT, JSONB[]) IS
    'Append N messages in one statement. Returns the generated ids.';


-- =============================================================================
--  2. dequeue -- atomically claim and lease a batch of messages
--
--  FOR UPDATE SKIP LOCKED is the whole point. Without SKIP LOCKED, a second
--  consumer running the same SELECT would BLOCK on the first consumer's row
--  locks, and N workers would serialise into one. With it, the second consumer
--  steps over the locked rows and claims the next unlocked ones, so N workers
--  drain N disjoint batches with no contention and no duplicate delivery.
--
--  Why the claim is done in a CTE and the UPDATE reads from it:
--    * ORDER BY ... LIMIT must be applied before locking, otherwise we would
--      lock (and skip) far more rows than the batch size.
--    * The outer UPDATE then only touches rows this transaction already holds a
--      lock on, so it can never block.
--
--  Only 'pending' rows are considered here. Reclaiming expired leases is the job
--  of reap_expired(): keeping the two apart lets this query be satisfied purely
--  by the partial index message_queue_ready_idx, walking it in id order and
--  stopping as soon as p_batch_size rows are claimed.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.dequeue(
    p_queue_name         TEXT,
    p_batch_size         INTEGER  DEFAULT 10,
    p_visibility_timeout INTERVAL DEFAULT INTERVAL '30 seconds'
) RETURNS TABLE (msg_id BIGINT, payload JSONB)
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    IF p_batch_size IS NULL OR p_batch_size < 1 THEN
        RAISE EXCEPTION 'dequeue: batch size must be >= 1, got %', p_batch_size;
    END IF;

    -- A NULL timeout would silently set locked_until = NULL (timestamptz + NULL),
    -- and `locked_until < now()` is then NULL, never true. The message would be
    -- stuck in 'processing' forever, invisible to both dequeue() and reap_expired().
    IF p_visibility_timeout IS NULL THEN
        RAISE EXCEPTION 'dequeue: visibility timeout must not be NULL';
    END IF;

    -- Every column reference below is alias-qualified. The OUT parameter named
    -- `payload` would otherwise collide with message_queue.payload and PL/pgSQL
    -- would abort with "column reference is ambiguous".
    RETURN QUERY
    WITH claimed AS (
        SELECT mq.id
        FROM   imdb.message_queue mq
        WHERE  mq.queue_name = p_queue_name
          AND  mq.status     = 'pending'
        ORDER  BY mq.id
        LIMIT  p_batch_size
        FOR UPDATE SKIP LOCKED
    ),
    leased AS (
        UPDATE imdb.message_queue mq
           SET status       = 'processing',
               locked_until = now() + p_visibility_timeout,
               updated_at   = now()
        FROM   claimed c
        WHERE  mq.id = c.id
        RETURNING mq.id, mq.payload
    )
    SELECT l.id, l.payload
    FROM   leased l
    ORDER  BY l.id;
END;
$$;

COMMENT ON FUNCTION imdb.dequeue(TEXT, INTEGER, INTERVAL) IS
    'Claim up to N pending messages via FOR UPDATE SKIP LOCKED and lease them.';


-- =============================================================================
--  3. ack -- mark a message as successfully processed
--
--  Returns TRUE only if this call actually performed the transition. FALSE means
--  the message was no longer 'processing' -- most likely the lease expired and
--  reap_expired() handed it to somebody else. A consumer that sees FALSE has
--  learned that its work may have been done twice; because the target inserts
--  use ON CONFLICT DO NOTHING, that is harmless.
--
--  (The brief's skeleton returns VOID. BOOLEAN is a strict superset:
--   `SELECT imdb.ack(1);` still works unchanged.)
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.ack(
    p_message_id BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    UPDATE imdb.message_queue mq
       SET status       = 'done',
           locked_until = NULL,
           last_error   = NULL,
           updated_at   = now()
     WHERE mq.id     = p_message_id
       AND mq.status = 'processing';

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END;
$$;

COMMENT ON FUNCTION imdb.ack(BIGINT) IS
    'processing -> done. FALSE if the lease had already been lost.';


-- =============================================================================
--  4. nack -- record a failure
--
--  Increments attempts. Under the retry ceiling the message goes back to
--  'pending' and will be redelivered; at or above it, the message is parked in
--  'failed' and never redelivered -- a poison message cannot spin forever.
--
--  Returns the resulting status ('pending' or 'failed'), or NULL if the message
--  was not in 'processing' and nothing was changed.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.nack(
    p_message_id    BIGINT,
    p_error_message TEXT    DEFAULT '',
    p_max_attempts  INTEGER DEFAULT 5
) RETURNS TEXT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_status TEXT;
BEGIN
    UPDATE imdb.message_queue mq
       SET attempts     = mq.attempts + 1,
           -- last_error is diagnostics, not payload. Cap it so a multi-megabyte
           -- Python traceback cannot bloat the queue heap.
           last_error   = left(coalesce(p_error_message, ''), 2000),
           status       = CASE
                              WHEN mq.attempts + 1 >= p_max_attempts THEN 'failed'
                              ELSE 'pending'
                          END,
           locked_until = NULL,
           updated_at   = now()
     WHERE mq.id     = p_message_id
       AND mq.status = 'processing'
    RETURNING mq.status INTO v_status;

    RETURN v_status;
END;
$$;

COMMENT ON FUNCTION imdb.nack(BIGINT, TEXT, INTEGER) IS
    'processing -> pending (retry) or failed (retries exhausted). Returns the new status.';


-- =============================================================================
--  5. reap_expired -- return abandoned leases to the queue
--
--  dequeue() commits the claim, so a crashed consumer leaves behind a
--  'processing' row that no transaction holds a lock on. Nothing would ever
--  reclaim it without this function.
--
--  SKIP LOCKED here guards against two reapers colliding, not against consumers.
--  Uses the partial index message_queue_stuck_idx.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.reap_expired(
    p_queue_name   TEXT,
    p_max_attempts INTEGER DEFAULT 5
) RETURNS INTEGER
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH expired AS (
        SELECT mq.id
        FROM   imdb.message_queue mq
        WHERE  mq.queue_name   = p_queue_name
          AND  mq.status       = 'processing'
          AND  mq.locked_until < now()
        ORDER  BY mq.id
        FOR UPDATE SKIP LOCKED
    )
    UPDATE imdb.message_queue mq
       SET attempts     = mq.attempts + 1,
           last_error   = 'lease expired before ack',
           status       = CASE
                              WHEN mq.attempts + 1 >= p_max_attempts THEN 'failed'
                              ELSE 'pending'
                          END,
           locked_until = NULL,
           updated_at   = now()
    FROM   expired e
    WHERE  mq.id = e.id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION imdb.reap_expired(TEXT, INTEGER) IS
    'Reclaim messages whose visibility timeout elapsed. Returns how many.';


-- =============================================================================
--  6. queue_stats -- one row of counters, used by the producer for backpressure
--
--  The producer polls `pending` and pauses whenever the backlog exceeds its
--  high-water mark. Without that, the producer races ahead of the consumer and
--  the queue's TOAST relation grows to hold the entire dataset at once.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.queue_stats(
    p_queue_name TEXT
) RETURNS TABLE (
    pending    BIGINT,
    processing BIGINT,
    done       BIGINT,
    failed     BIGINT,
    total      BIGINT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT count(*) FILTER (WHERE mq.status = 'pending'),
           count(*) FILTER (WHERE mq.status = 'processing'),
           count(*) FILTER (WHERE mq.status = 'done'),
           count(*) FILTER (WHERE mq.status = 'failed'),
           count(*)
    FROM   imdb.message_queue mq
    WHERE  mq.queue_name = p_queue_name;
END;
$$;

COMMENT ON FUNCTION imdb.queue_stats(TEXT) IS
    'Per-status counters for one queue. Drives producer backpressure.';


-- =============================================================================
--  7. purge_done -- bounded delete of acked messages
--
--  Keeping every 'done' message would defeat the whole storage argument: the
--  batched payloads are what we are trying not to accumulate. Deleting in
--  bounded chunks keeps each transaction short and lets autovacuum keep up.
--
--  Returns the number of rows deleted; call it in a loop until it returns 0.
-- =============================================================================
CREATE OR REPLACE FUNCTION imdb.purge_done(
    p_queue_name TEXT,
    p_older_than INTERVAL DEFAULT INTERVAL '0 seconds',
    p_limit      INTEGER  DEFAULT 10000
) RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    WITH victims AS (
        SELECT mq.id
        FROM   imdb.message_queue mq
        WHERE  mq.queue_name = p_queue_name
          AND  mq.status     = 'done'
          AND  mq.updated_at < now() - p_older_than
        ORDER  BY mq.id
        LIMIT  p_limit
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM imdb.message_queue mq
    USING  victims v
    WHERE  mq.id = v.id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

COMMENT ON FUNCTION imdb.purge_done(TEXT, INTERVAL, INTEGER) IS
    'Delete up to N acked messages older than the given age. Call until it returns 0.';
