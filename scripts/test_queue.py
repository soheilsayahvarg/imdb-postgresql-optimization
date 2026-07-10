#!/usr/bin/env python3
"""
End-to-end verification of the imdb message queue (Part B).

Run after applying sql/01_queue_functions.sql:

    python scripts/test_queue.py

Exits non-zero if any check fails.

Each test owns a private queue name so that leftovers from one test can never be
claimed by the next. Everything under 'test_%' and 'toast_%' is deleted on entry
and exit.

What it proves, in order:
  1. The brief's own smoke test runs verbatim.
  2. FOR UPDATE SKIP LOCKED lets two consumers drain disjoint batches without
     blocking -- and the same query WITHOUT skip locked really does block.
  3. An abandoned lease is reclaimed, and the crashed consumer's late ack() is
     rejected. This is the at-least-once delivery boundary.
  4. nack() retries under the ceiling and parks the message as 'failed' at it.
  5. A realistic ingestion round trip for both payload shapes, including the
     savepoint-per-batch pattern that stops one bad row killing 1000 good ones.
  6. The storage claim behind the whole design: batched payloads are TOASTed and
     lz4-compressed, single-row payloads are not.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.errors
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "resources" / ".env")

MAX_ATTEMPTS = 3

_passed: list[str] = []
_failed: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    (_passed if ok else _failed).append(name)
    line = f"  [{'PASS' if ok else 'FAIL'}] {name}"
    if detail:
        line += f"  ({detail})"
    print(line)


def connect(autocommit: bool = True):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "imdb"),
        user=os.getenv("PGUSER", "imdb"),
        password=os.getenv("PGPASSWORD", "imdb_pass"),
    )
    conn.autocommit = autocommit
    return conn


# -----------------------------------------------------------------------------
#  The consumer logic that Part D will reuse.
# -----------------------------------------------------------------------------

INSERT_BASICS = (
    "INSERT INTO imdb.title_basics "
    "(tconst, title_type, primary_title, original_title, is_adult,"
    " start_year, end_year, runtime_minutes, genres, metadata) "
    "VALUES %s ON CONFLICT (tconst) DO NOTHING"
)


def rows_of(payload: dict) -> list[dict]:
    """Accept both payload shapes: the brief's single record, and our batch."""
    if "rows" in payload:
        return payload["rows"]
    if "data" in payload:
        return [payload["data"]]
    raise ValueError(f"payload has neither 'data' nor 'rows': {sorted(payload)}")


def to_tuple(row: dict) -> tuple:
    meta = row.get("metadata")
    return (
        row["tconst"], row.get("title_type"), row.get("primary_title"),
        row.get("original_title"), row.get("is_adult"), row.get("start_year"),
        row.get("end_year"), row.get("runtime_minutes"), row.get("genres"),
        Json(meta) if meta is not None else None,
    )


# Synthetic rows must live in a tconst namespace that real IMDb data can never
# occupy. Real ids are 'tt' + digits and already run past tt36000000, so a prefix
# like 'tt99' collides with tens of thousands of genuine titles -- and the cleanup
# DELETE would silently destroy them once the dataset is loaded. tconst has no
# format constraint, so any non-'tt' prefix is safe.
TEST_PREFIX = "qtest"


def make_row(n: int) -> dict:
    return {
        "tconst": f"{TEST_PREFIX}{n:08d}",
        "title_type": "movie",
        "primary_title": f"Synthetic Test Movie Number {n}",
        "original_title": f"Synthetic Test Movie Number {n}",
        "is_adult": False,
        "start_year": 2000 + (n % 25),
        "end_year": None,
        "runtime_minutes": 90 + (n % 60),
        "genres": ["Action", "Drama"],
        "metadata": {"source": "test_queue.py", "seq": n},
    }


# -----------------------------------------------------------------------------
#  Helpers
# -----------------------------------------------------------------------------

def reset(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM imdb.message_queue "
                    "WHERE queue_name LIKE %s OR queue_name LIKE %s",
                    ("test\\_%", "toast\\_%"))
        cur.execute("DELETE FROM imdb.title_basics WHERE tconst LIKE %s",
                    (TEST_PREFIX + "%",))


def enqueue(cur, queue: str, payload: dict) -> int:
    cur.execute("SELECT imdb.enqueue(%s, %s::jsonb)", (queue, json.dumps(payload)))
    return cur.fetchone()[0]


def status_of(cur, msg_id: int) -> tuple[str, int]:
    cur.execute("SELECT status, attempts FROM imdb.message_queue WHERE id = %s", (msg_id,))
    return cur.fetchone()


# =============================================================================
#  1. The brief's smoke test, verbatim
# =============================================================================
def test_brief_sequence(conn) -> None:
    print("\n1. Brief's smoke test")
    q = "test_brief"
    with conn.cursor() as cur:
        msg_id = enqueue(cur, q, {"test": "hello"})

        cur.execute("SELECT msg_id, payload FROM imdb.dequeue(%s, 1, '1 minute')", (q,))
        rows = cur.fetchall()
        check("dequeue returns exactly the enqueued message",
              rows == [(msg_id, {"test": "hello"})], f"got {rows}")

        cur.execute("SELECT imdb.ack(%s)", (msg_id,))
        check("ack() returns TRUE on a live lease", cur.fetchone()[0] is True)

        st, attempts = status_of(cur, msg_id)
        check("status is 'done' and attempts stayed 0",
              st == "done" and attempts == 0, f"status={st} attempts={attempts}")

        cur.execute("SELECT count(*) FROM imdb.dequeue(%s, 10, '1 minute')", (q,))
        check("a done message is never redelivered", cur.fetchone()[0] == 0)


# =============================================================================
#  2. SKIP LOCKED: the load-bearing claim
# =============================================================================
def test_skip_locked(conn_tx, conn_auto) -> None:
    print("\n2. FOR UPDATE SKIP LOCKED under concurrency")
    q = "test_skip"
    with conn_auto.cursor() as cur:
        ids = [enqueue(cur, q, {"n": i}) for i in range(10)]

    # Consumer A claims 5 and keeps its transaction OPEN, so its row locks stand.
    cur_a = conn_tx.cursor()
    cur_a.execute("SELECT msg_id FROM imdb.dequeue(%s, 5, '5 minutes')", (q,))
    a_ids = [r[0] for r in cur_a.fetchall()]

    # Consumer B must step over A's locked rows rather than queue up behind them.
    started = time.monotonic()
    with conn_auto.cursor() as cur_b:
        cur_b.execute("SELECT msg_id FROM imdb.dequeue(%s, 5, '5 minutes')", (q,))
        b_ids = [r[0] for r in cur_b.fetchall()]
    elapsed = time.monotonic() - started

    check("consumer B did not block on A's locks", elapsed < 2.0, f"{elapsed:.3f}s")
    check("B claimed a full batch of 5", len(b_ids) == 5, f"got {len(b_ids)}")
    check("the two batches are disjoint", set(a_ids).isdisjoint(b_ids))
    check("together they cover every enqueued message", sorted(a_ids + b_ids) == sorted(ids))

    # Control. A's 5 rows are still 'pending' in B's snapshot (A never committed)
    # but they are row-locked, so a plain FOR UPDATE has to wait -- and times out.
    # Without SKIP LOCKED, N consumers would serialise into one.
    blocked = False
    with conn_auto.cursor() as cur_b:
        cur_b.execute("SET statement_timeout = '1500ms'")
        try:
            cur_b.execute(
                "SELECT id FROM imdb.message_queue"
                " WHERE queue_name = %s AND status = 'pending'"
                " ORDER BY id LIMIT 5 FOR UPDATE", (q,))
            cur_b.fetchall()
        except psycopg2.errors.QueryCanceled:
            blocked = True
    conn_auto.rollback()
    with conn_auto.cursor() as cur_b:
        cur_b.execute("SET statement_timeout = 0")

    check("control: plain FOR UPDATE does block -- SKIP LOCKED is what saves us", blocked)

    conn_tx.rollback()  # release A's locks


# =============================================================================
#  3. Lease expiry, reaping, and the at-least-once boundary
# =============================================================================
def test_lease_expiry(conn) -> None:
    print("\n3. Visibility timeout and reap_expired()")
    q = "test_lease"
    with conn.cursor() as cur:
        msg_id = enqueue(cur, q, {"work": "slow"})

        cur.execute("SELECT msg_id FROM imdb.dequeue(%s, 1, '1 second')", (q,))
        check("message was leased", cur.fetchone()[0] == msg_id)

        cur.execute("SELECT imdb.reap_expired(%s, %s)", (q, MAX_ATTEMPTS))
        check("nothing is reaped while the lease is still valid", cur.fetchone()[0] == 0)

        time.sleep(1.5)

        cur.execute("SELECT imdb.reap_expired(%s, %s)", (q, MAX_ATTEMPTS))
        check("the expired lease is reclaimed", cur.fetchone()[0] == 1)

        st, attempts = status_of(cur, msg_id)
        check("reaped message is pending again, attempts incremented",
              st == "pending" and attempts == 1, f"status={st} attempts={attempts}")

        # The crashed consumer wakes up and acks. It must be told 'no'.
        cur.execute("SELECT imdb.ack(%s)", (msg_id,))
        check("a late ack() from the evicted consumer returns FALSE",
              cur.fetchone()[0] is False)

        st, _ = status_of(cur, msg_id)
        check("...and the message remains claimable", st == "pending")


# =============================================================================
#  4. nack(): retry, then park
# =============================================================================
def test_nack_retries(conn) -> None:
    print("\n4. nack() retry ceiling")
    q = "test_nack"
    with conn.cursor() as cur:
        msg_id = enqueue(cur, q, {"poison": True})

        for attempt in range(1, MAX_ATTEMPTS + 1):
            cur.execute("SELECT msg_id FROM imdb.dequeue(%s, 1, '1 minute')", (q,))
            claimed = cur.fetchall()
            check(f"attempt {attempt}: poison message is redelivered",
                  claimed == [(msg_id,)], f"got {claimed}")

            cur.execute("SELECT imdb.nack(%s, %s, %s)",
                        (msg_id, f"synthetic failure {attempt}", MAX_ATTEMPTS))
            new_status = cur.fetchone()[0]
            expected = "failed" if attempt >= MAX_ATTEMPTS else "pending"
            check(f"attempt {attempt}: nack() -> '{expected}'",
                  new_status == expected, f"got '{new_status}'")

        st, attempts = status_of(cur, msg_id)
        check("exhausted message is parked as failed",
              st == "failed" and attempts == MAX_ATTEMPTS,
              f"status={st} attempts={attempts}")

        cur.execute("SELECT count(*) FROM imdb.dequeue(%s, 10, '1 minute')", (q,))
        check("a failed message is never redelivered", cur.fetchone()[0] == 0)

        cur.execute("SELECT last_error FROM imdb.message_queue WHERE id = %s", (msg_id,))
        check("last_error was recorded",
              cur.fetchone()[0] == f"synthetic failure {MAX_ATTEMPTS}")


# =============================================================================
#  5. Realistic ingestion: both payload shapes + savepoint-per-batch
# =============================================================================
def test_ingestion(conn_auto) -> None:
    print("\n5. Ingestion round trip (single shape, batch shape, poison batch)")
    q = "test_ingest"
    with conn_auto.cursor() as cur:
        single_id = enqueue(cur, q, {"table": "title_basics", "data": make_row(1)})
        batch_id = enqueue(cur, q, {"table": "title_basics",
                                    "rows": [make_row(n) for n in range(2, 12)]})
        # One row in this batch carries a start_year that cannot fit in SMALLINT.
        bad = make_row(99)
        bad["start_year"] = 999999
        poison_id = enqueue(cur, q, {"table": "title_basics",
                                     "rows": [make_row(50), bad, make_row(51)]})

    acked, nacked = [], []
    conn = connect(autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SELECT msg_id, payload FROM imdb.dequeue(%s, 10, '2 minutes')", (q,))
        for msg_id, payload in cur.fetchall():
            cur.execute("SAVEPOINT batch")
            try:
                execute_values(cur, INSERT_BASICS, [to_tuple(r) for r in rows_of(payload)])
                cur.execute("RELEASE SAVEPOINT batch")
                cur.execute("SELECT imdb.ack(%s)", (msg_id,))
                acked.append(msg_id)
            except psycopg2.Error as exc:
                # Undo only this batch. The transaction, and the claim on every
                # other message in it, survives.
                cur.execute("ROLLBACK TO SAVEPOINT batch")
                cur.execute("SELECT imdb.nack(%s, %s, %s)",
                            (msg_id, str(exc).strip(), MAX_ATTEMPTS))
                nacked.append(msg_id)
        conn.commit()
    conn.close()

    check("the single-record message was acked", single_id in acked)
    check("the batch message was acked", batch_id in acked)
    check("the poison batch was nacked, not acked", poison_id in nacked)

    with conn_auto.cursor() as cur:
        cur.execute("SELECT count(*) FROM imdb.title_basics WHERE tconst LIKE %s",
                    (TEST_PREFIX + "%",))
        landed = cur.fetchone()[0]
        check("exactly the 11 good rows landed; the poison batch inserted nothing",
              landed == 11, f"got {landed}")

        # Replay. Idempotency is what makes at-least-once delivery safe.
        execute_values(cur, INSERT_BASICS, [to_tuple(make_row(n)) for n in range(1, 12)])
        cur.execute("SELECT count(*) FROM imdb.title_basics WHERE tconst LIKE %s",
                    (TEST_PREFIX + "%",))
        check("replaying an already-processed batch inserts nothing (ON CONFLICT DO NOTHING)",
              cur.fetchone()[0] == landed)


# =============================================================================
#  6. The storage claim: does batching actually trigger TOAST + lz4?
# =============================================================================
def queue_bytes(cur) -> tuple[int, int]:
    cur.execute("""
        SELECT pg_relation_size(c.oid),
               COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0)
        FROM   pg_class c
        WHERE  c.oid = 'imdb.message_queue'::regclass
    """)
    return cur.fetchone()


def test_toast_behaviour(conn) -> None:
    print("\n6. TOAST behaviour: 2000 single-row messages vs 2 x 1000-row batches")
    n_rows = 2000
    with conn.cursor() as cur:
        cur.execute("SELECT attcompression FROM pg_attribute"
                    " WHERE attrelid = 'imdb.message_queue'::regclass AND attname = 'payload'")
        check("payload column is explicitly lz4-compressed", cur.fetchone()[0] == "l")

        cur.execute("VACUUM (FULL) imdb.message_queue")
        heap0, toast0 = queue_bytes(cur)

        for n in range(n_rows):
            enqueue(cur, "toast_single", {"table": "title_basics", "data": make_row(n)})
        heap1, toast1 = queue_bytes(cur)

        payloads = [{"table": "title_basics",
                     "rows": [make_row(n) for n in range(i, i + 1000)]} for i in (0, 1000)]
        # ARRAY(SELECT jsonb_array_elements(...)) builds a jsonb[] with no
        # dependence on how the driver types a Python list of strings.
        cur.execute("SELECT count(*) FROM imdb.enqueue_many"
                    "(%s, ARRAY(SELECT jsonb_array_elements(%s::jsonb)))",
                    ("toast_batch", json.dumps(payloads)))
        check("enqueue_many inserted both batch messages", cur.fetchone()[0] == 2)
        heap2, toast2 = queue_bytes(cur)

        single = (heap1 - heap0) + (toast1 - toast0)
        batch = (heap2 - heap1) + (toast2 - toast1)

        print(f"     single-row messages : {single:>10,} bytes "
              f"(heap {heap1 - heap0:,} / toast {toast1 - toast0:,})")
        print(f"     batched messages    : {batch:>10,} bytes "
              f"(heap {heap2 - heap1:,} / toast {toast2 - toast1:,})")
        print(f"     bytes per data row  : single {single / n_rows:,.1f}"
              f"    batched {batch / n_rows:,.1f}")
        if batch:
            print(f"     ratio               : {single / batch:,.1f}x smaller when batched")

        check("single-row payloads stay inline (TOAST relation does not grow)",
              (toast1 - toast0) == 0, f"toast grew by {toast1 - toast0}")
        check("batched payloads are pushed out to the TOAST relation",
              (toast2 - toast1) > 0, f"toast grew by {toast2 - toast1}")
        check("batching stores identical data in strictly less space",
              batch < single, f"batch={batch:,} single={single:,}")


# =============================================================================


def main() -> int:
    conn_auto = connect(autocommit=True)
    conn_tx = connect(autocommit=False)
    try:
        reset(conn_auto)
        test_brief_sequence(conn_auto)
        test_skip_locked(conn_tx, conn_auto)
        test_lease_expiry(conn_auto)
        test_nack_retries(conn_auto)
        test_ingestion(conn_auto)
        test_toast_behaviour(conn_auto)
    finally:
        # If a test raised while conn_tx still held claimed rows, reset()'s DELETE
        # would block on those locks forever -- the server sets no statement_timeout.
        # Release them before touching the table.
        conn_tx.rollback()
        reset(conn_auto)
        conn_tx.close()
        conn_auto.close()

    print(f"\n{len(_passed)} passed, {len(_failed)} failed")
    for name in _failed:
        print(f"  FAILED: {name}")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
