-- =============================================================================
--  Pure-SQL smoke test for the queue functions.
--
--      docker exec -i imdb_postgres psql -U imdb -d imdb -v ON_ERROR_STOP=1 \
--          < sql/01_queue_smoke_test.sql
--
--  Self-asserting: every expectation raises an exception on failure, so a clean
--  run that prints "ALL SMOKE TESTS PASSED" is the pass condition. The whole
--  thing runs inside a transaction that is rolled back, so it leaves no rows
--  behind. (Only the identity sequence advances, which is harmless.)
--
--  What it cannot cover: concurrency. Proving that SKIP LOCKED stops two
--  consumers blocking each other needs two sessions -- see scripts/test_queue.py.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DELETE FROM imdb.message_queue WHERE queue_name = 'smoke_test';

DO $$
DECLARE
    v_id      BIGINT;
    v_msg     BIGINT;
    v_payload JSONB;
    v_status  TEXT;
    v_ok      BOOLEAN;
    v_count   INTEGER;
BEGIN
    -- --- enqueue ------------------------------------------------------------
    v_id := imdb.enqueue('smoke_test', '{"test": "hello"}'::JSONB);
    IF v_id IS NULL THEN
        RAISE EXCEPTION 'enqueue() returned NULL';
    END IF;

    -- --- dequeue ------------------------------------------------------------
    SELECT d.msg_id, d.payload INTO v_msg, v_payload
    FROM   imdb.dequeue('smoke_test', 1, '1 minute') AS d;

    IF v_msg IS DISTINCT FROM v_id THEN
        RAISE EXCEPTION 'dequeue() returned id % but enqueue() gave %', v_msg, v_id;
    END IF;
    IF v_payload <> '{"test": "hello"}'::JSONB THEN
        RAISE EXCEPTION 'payload did not round-trip: %', v_payload;
    END IF;

    SELECT mq.status INTO v_status FROM imdb.message_queue mq WHERE mq.id = v_id;
    IF v_status <> 'processing' THEN
        RAISE EXCEPTION 'after dequeue expected processing, got %', v_status;
    END IF;

    -- A leased message must not be handed to anybody else.
    SELECT count(*) INTO v_count FROM imdb.dequeue('smoke_test', 10, '1 minute');
    IF v_count <> 0 THEN
        RAISE EXCEPTION 'a leased message was redelivered';
    END IF;

    -- --- ack ----------------------------------------------------------------
    v_ok := imdb.ack(v_id);
    IF NOT v_ok THEN
        RAISE EXCEPTION 'ack() returned FALSE on a live lease';
    END IF;

    SELECT mq.status, mq.attempts INTO v_status, v_count
    FROM imdb.message_queue mq WHERE mq.id = v_id;
    IF v_status <> 'done' OR v_count <> 0 THEN
        RAISE EXCEPTION 'after ack expected done/0, got %/%', v_status, v_count;
    END IF;

    -- Acking twice changes nothing and says so.
    v_ok := imdb.ack(v_id);
    IF v_ok THEN
        RAISE EXCEPTION 'a second ack() on the same message returned TRUE';
    END IF;

    -- --- nack: retry, then park ---------------------------------------------
    v_id := imdb.enqueue('smoke_test', '{"poison": true}'::JSONB);

    PERFORM * FROM imdb.dequeue('smoke_test', 1, '1 minute');
    v_status := imdb.nack(v_id, 'first failure', 2);
    IF v_status <> 'pending' THEN
        RAISE EXCEPTION 'first nack() should requeue, got %', v_status;
    END IF;

    PERFORM * FROM imdb.dequeue('smoke_test', 1, '1 minute');
    v_status := imdb.nack(v_id, 'second failure', 2);
    IF v_status <> 'failed' THEN
        RAISE EXCEPTION 'second nack() should park the message, got %', v_status;
    END IF;

    SELECT count(*) INTO v_count FROM imdb.dequeue('smoke_test', 10, '1 minute');
    IF v_count <> 0 THEN
        RAISE EXCEPTION 'a failed message was redelivered';
    END IF;

    -- --- reap_expired -------------------------------------------------------
    -- A negative visibility timeout leases the message into the past, which is
    -- exactly the state a crashed consumer leaves behind.
    v_id := imdb.enqueue('smoke_test', '{"crash": true}'::JSONB);
    PERFORM * FROM imdb.dequeue('smoke_test', 1, '-1 second');

    IF imdb.reap_expired('smoke_test', 5) <> 1 THEN
        RAISE EXCEPTION 'reap_expired() did not reclaim the abandoned lease';
    END IF;

    SELECT mq.status, mq.attempts INTO v_status, v_count
    FROM imdb.message_queue mq WHERE mq.id = v_id;
    IF v_status <> 'pending' OR v_count <> 1 THEN
        RAISE EXCEPTION 'after reap expected pending/1, got %/%', v_status, v_count;
    END IF;

    -- The crashed consumer's late ack must be rejected: it no longer owns the row.
    v_ok := imdb.ack(v_id);
    IF v_ok THEN
        RAISE EXCEPTION 'a late ack() after lease expiry returned TRUE';
    END IF;

    -- --- queue_stats --------------------------------------------------------
    SELECT s.total INTO v_count FROM imdb.queue_stats('smoke_test') AS s;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'queue_stats total expected 3, got %', v_count;
    END IF;

    RAISE NOTICE 'ALL SMOKE TESTS PASSED';
END
$$;

ROLLBACK;
