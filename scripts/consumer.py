#!/usr/bin/env python3
"""
Streaming consumer: imdb.message_queue -> imdb.* tables

    python scripts/consumer.py                    # drain the default queue and exit when empty
    python scripts/consumer.py --workers 3        # three concurrent drainers in one process
    python scripts/consumer.py --follow           # never exit on empty; wait for more work
    python scripts/consumer.py --selftest         # unit-test the pure logic, no database

Delivery model (model B, matching the brief's skeleton):

  dequeue() runs on an autocommit connection, so the 'pending' -> 'processing'
  lease is COMMITTED before the rows are inserted. The insert then happens in a
  separate transaction on a second connection, and ack() commits afterwards.

  This creates an at-least-once window: a consumer that commits its inserts and
  then dies before ack() leaves the message in 'processing'. reap_expired() hands
  it back once the visibility timeout lapses, and another consumer re-inserts the
  same rows. That is safe only because every INSERT is ON CONFLICT DO NOTHING --
  the idempotency and the at-least-once delivery are two halves of one argument.
  (Model A -- dequeue + insert + ack in one transaction -- would make reap_expired
  dead code, because no 'processing' row would ever be committed.)

Per-message fault isolation:

  A dequeued message is inserted with execute_values inside a SAVEPOINT. If the
  batch fails on a genuine data error, the savepoint is rolled back and the rows
  are retried one at a time, each under its own savepoint. The healthy 999 land;
  each unhealthy row is rolled back individually and written to imdb.dead_letter
  with its SQLSTATE. Because dead_letter records the failing row precisely, we do
  NOT nack the whole 1000-row message for one bad row -- a message-level nack would
  pointlessly re-drive the 999 good rows through the queue. nack() is reserved for
  the cases it actually fits: a transient failure of the whole batch (deadlock /
  serialization -> retry), and a single-row message that is entirely unprocessable.
"""

from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import psycopg2
import psycopg2.errors
from psycopg2.extras import Json, execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (DEFAULT_QUEUE, ROOT, connect, human_bytes,  # noqa: E402
                    DATASETS, REQUIRED)

RESULTS_DIR = ROOT / "results" / "timings"

SHUTDOWN = threading.Event()
_sigint_count = 0


def _on_sigint(signum, frame):
    global _sigint_count
    _sigint_count += 1
    if _sigint_count >= 2:
        raise KeyboardInterrupt
    SHUTDOWN.set()
    print("\n  Ctrl-C: draining the current batch, then stopping...", flush=True)


def wait_shutdown(seconds: float) -> bool:
    """Sleep that a signal cuts short. Returns True if shutdown was requested."""
    return SHUTDOWN.wait(seconds)


# =============================================================================
#  Target tables
#
#  Column order here IS the order adapt() emits and the order the INSERT lists.
#  `jsonb` names the columns that must be wrapped in psycopg2's Json adapter;
#  TEXT[] columns (genres, directors, ...) are adapted from Python lists natively.
# =============================================================================

TARGETS: dict[str, dict] = {
    "title_basics": {
        "cols": ["tconst", "title_type", "primary_title", "original_title", "is_adult",
                 "start_year", "end_year", "runtime_minutes", "genres", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(tconst)",
    },
    "title_ratings": {
        "cols": ["tconst", "average_rating", "num_votes", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(tconst)",
    },
    "title_crew": {
        "cols": ["tconst", "directors", "writers", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(tconst)",
    },
    "title_episode": {
        "cols": ["tconst", "parent_tconst", "season_number", "episode_number", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(tconst)",
    },
    "name_basics": {
        "cols": ["nconst", "primary_name", "birth_year", "death_year",
                 "primary_profession", "known_for_titles", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(nconst)",
    },
    "title_principals": {
        "cols": ["tconst", "ordering", "nconst", "category", "job", "characters", "metadata"],
        "jsonb": {"characters", "metadata"},
        "conflict": "(tconst, ordering)",
    },
    "title_akas": {
        "cols": ["ordering", "title_id", "title", "region", "language",
                 "types", "attributes", "is_original_title", "metadata"],
        "jsonb": {"metadata"},
        "conflict": "(title_id, ordering)",
    },
}

INSERTS = {
    t: f"INSERT INTO imdb.{t} ({', '.join(spec['cols'])}) "
       f"VALUES %s ON CONFLICT {spec['conflict']} DO NOTHING"
    for t, spec in TARGETS.items()
}

DEAD_LETTER_INSERT = (
    "INSERT INTO imdb.dead_letter (table_name, payload, sqlstate, error) VALUES %s"
)


def adapt(table: str, row: dict) -> tuple:
    """Row dict -> value tuple in column order, wrapping only the JSONB columns."""
    spec = TARGETS[table]
    jsonb = spec["jsonb"]
    out = []
    for col in spec["cols"]:
        v = row.get(col)
        out.append(Json(v) if (col in jsonb and v is not None) else v)
    return tuple(out)


def split_payload(payload) -> tuple[str | None, list | None]:
    """
    Accept both envelope shapes.

    psycopg2 parses jsonb to a dict by default, but a str is handled too so the
    consumer does not depend on the typecaster being registered.
    """
    if isinstance(payload, (str, bytes, bytearray)):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        return None, None
    table = payload.get("table")
    if "rows" in payload:
        rows = payload["rows"]
    elif "data" in payload:
        rows = [payload["data"]]
    else:
        rows = None
    if not isinstance(rows, list):
        rows = None
    return table, rows


def is_transient(exc: psycopg2.Error) -> bool:
    """SQLSTATE class 40: serialization_failure (40001), deadlock_detected (40P01)."""
    code = getattr(exc, "pgcode", None) or ""
    return code.startswith("40")


def short(exc: BaseException, limit: int = 500) -> str:
    return str(exc).strip().replace("\n", " ")[:limit]


# =============================================================================
#  Ingesting one message
# =============================================================================

def _rowcount(cur, fallback: int) -> int:
    n = cur.rowcount
    return n if n is not None and n >= 0 else fallback


def ingest_message(work, table: str, rows: list[dict]) -> dict:
    """
    Insert one message's rows. Commits the work connection.

    Returns a disposition:
      action   : 'ack' or 'nack'   (applied by the caller on the lease connection)
      error    : nack reason, or None
      inserted : rows newly inserted (ON CONFLICT skips are not counted)
      skipped  : rows that hit an existing key (proof idempotency is doing its job)
      bad      : rows sent to dead_letter
    """
    if table not in INSERTS:
        return {"action": "nack", "error": f"unknown table {table!r}",
                "inserted": 0, "skipped": 0, "bad": len(rows)}

    tmpl = INSERTS[table]
    values = [adapt(table, r) for r in rows]

    cur = work.cursor()
    try:
        cur.execute("SAVEPOINT ins")
        execute_values(cur, tmpl, values, page_size=max(len(values), 1))
        inserted = _rowcount(cur, len(values))
        cur.execute("RELEASE SAVEPOINT ins")
        work.commit()
        return {"action": "ack", "error": None, "inserted": inserted,
                "skipped": len(values) - inserted, "bad": 0}
    except psycopg2.Error as exc:
        work.rollback()
        if is_transient(exc):
            # The whole batch hit a deadlock/serialization failure. Retrying the
            # batch is the right move, so hand it back to the queue intact.
            return {"action": "nack", "error": short(exc),
                    "inserted": 0, "skipped": 0, "bad": 0}

    # A genuine data error somewhere in the batch. Isolate it row by row.
    inserted = 0
    bad: list[tuple[dict, str, str]] = []
    cur = work.cursor()
    for row, value in zip(rows, values):
        try:
            cur.execute("SAVEPOINT one")
            execute_values(cur, tmpl, [value], page_size=1)
            inserted += _rowcount(cur, 1)
            cur.execute("RELEASE SAVEPOINT one")
        except psycopg2.Error as exc:
            cur.execute("ROLLBACK TO SAVEPOINT one")
            if is_transient(exc):
                # A deadlock/serialization failure is NOT this row's fault. Do not
                # dead-letter a healthy row -- abandon the whole batch and let the
                # queue redeliver it. (Good rows re-inserted later ON CONFLICT skip.)
                work.rollback()
                return {"action": "nack", "error": short(exc),
                        "inserted": 0, "skipped": 0, "bad": 0}
            bad.append((row, getattr(exc, "pgcode", None) or "", short(exc)))

    if bad:
        # Dead-lettering under its own savepoint: if this INSERT itself fails
        # (say the payload is enormous), the healthy rows already inserted in this
        # transaction must still survive the commit.
        try:
            cur.execute("SAVEPOINT dl")
            execute_values(cur, DEAD_LETTER_INSERT,
                           [(table, Json(r), code, err) for (r, code, err) in bad],
                           page_size=max(len(bad), 1))
            cur.execute("RELEASE SAVEPOINT dl")
        except psycopg2.Error:
            cur.execute("ROLLBACK TO SAVEPOINT dl")
    work.commit()

    skipped = len(rows) - inserted - len(bad)
    if bad and len(rows) == 1:
        # A one-row message that is entirely unprocessable. dead_letter already has
        # the row; nack drives the queue-level failure signal so it lands in 'failed'.
        return {"action": "nack", "error": bad[0][2],
                "inserted": inserted, "skipped": skipped, "bad": len(bad)}
    return {"action": "ack", "error": None,
            "inserted": inserted, "skipped": skipped, "bad": len(bad)}


# =============================================================================
#  Shared, thread-safe run state
# =============================================================================

class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.messages = 0
        self.inserted = defaultdict(int)
        self.skipped = defaultdict(int)
        self.bad = 0
        self.nacked = 0
        self.malformed = 0
        self.started = time.monotonic()
        self._last_activity = time.monotonic()

    def record(self, table: str, res: dict) -> None:
        with self._lock:
            self.messages += 1
            self.inserted[table] += res.get("inserted", 0)
            self.skipped[table] += res.get("skipped", 0)
            self.bad += res.get("bad", 0)
            if res["action"] == "nack":
                self.nacked += 1
            self._last_activity = time.monotonic()

    def note_malformed(self) -> None:
        with self._lock:
            self.malformed += 1
            self.messages += 1

    def mark_active(self) -> None:
        with self._lock:
            self._last_activity = time.monotonic()

    def idle_seconds(self) -> float:
        with self._lock:
            return time.monotonic() - self._last_activity

    def total_inserted(self) -> int:
        with self._lock:
            return sum(self.inserted.values())


# =============================================================================
#  Queue helpers (all on an autocommit "lease" connection)
# =============================================================================

def dequeue(lease, queue: str, batch: int, visibility: str) -> list[tuple[int, object]]:
    with lease.cursor() as cur:
        cur.execute("SELECT msg_id, payload FROM imdb.dequeue(%s, %s, %s::interval)",
                    (queue, batch, visibility))
        return cur.fetchall()


def ack(lease, msg_id: int) -> None:
    with lease.cursor() as cur:
        cur.execute("SELECT imdb.ack(%s)", (msg_id,))


def nack(lease, msg_id: int, error: str, max_attempts: int) -> None:
    with lease.cursor() as cur:
        cur.execute("SELECT imdb.nack(%s, %s, %s)", (msg_id, error, max_attempts))


def queue_stats(lease, queue: str) -> dict:
    with lease.cursor() as cur:
        cur.execute("SELECT pending, processing, done, failed, total"
                    "  FROM imdb.queue_stats(%s)", (queue,))
        p, pr, d, f, t = cur.fetchone()
    return {"pending": p, "processing": pr, "done": d, "failed": f, "total": t}


def producer_completion(lease) -> tuple[bool, bool]:
    """
    (all_completed, has_info).

    has_info is False when there is no producer_progress table, or it is empty --
    i.e. the consumer is running standalone and cannot infer the producer's plan.
    all_completed is True only when every table the producer has registered so far
    is marked completed. It is deliberately NOT a subset test against a fixed
    expected list, so a consumer with --all does not deadlock waiting for a table
    (title_akas) the producer was never asked to emit.
    """
    with lease.cursor() as cur:
        cur.execute("SELECT to_regclass('imdb.producer_progress')")
        if cur.fetchone()[0] is None:
            return False, False
        cur.execute("SELECT bool_and(completed), count(*) FROM imdb.producer_progress")
        all_completed, n = cur.fetchone()
    if not n:
        return False, False
    return bool(all_completed), True


def completed_tables(lease) -> set[str]:
    """The set of tables the producer has marked completed (empty if none / no info)."""
    with lease.cursor() as cur:
        cur.execute("SELECT to_regclass('imdb.producer_progress')")
        if cur.fetchone()[0] is None:
            return set()
        cur.execute("SELECT table_name FROM imdb.producer_progress WHERE completed")
        return {t for (t,) in cur.fetchall()}


def pending_for_table(lease, queue: str, table: str) -> int:
    """How many messages for `table` are still pending or processing."""
    with lease.cursor() as cur:
        cur.execute("SELECT count(*) FROM imdb.message_queue"
                    " WHERE queue_name = %s AND status IN ('pending', 'processing')"
                    "   AND payload->>'table' = %s", (queue, table))
        return cur.fetchone()[0]


# =============================================================================
#  Physical-size measurement (the numbers the LaTeX report quotes)
# =============================================================================

def measure_table(lease, table: str, exact_rows: bool) -> dict:
    qualified = f"imdb.{table}"
    with lease.cursor() as cur:
        cur.execute("""
            SELECT pg_total_relation_size(c.oid),
                   pg_relation_size(c.oid),
                   pg_indexes_size(c.oid),
                   COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0)
            FROM   pg_class c
            WHERE  c.oid = %s::regclass
        """, (qualified,))
        total, heap, indexes, toast = cur.fetchone()
        rows = None
        if not exact_rows:
            cur.execute("SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
                        (qualified,))
            est = cur.fetchone()[0]
            # reltuples is -1 for a never-ANALYZEd relation in PG14+. A freshly
            # loaded table has not been analyzed yet, so fall back to an exact
            # count rather than reporting 0 rows in the size table.
            rows = est if (est is not None and est >= 0) else None
        if rows is None:
            cur.execute(f"SELECT count(*) FROM {qualified}")
            rows = cur.fetchone()[0]
    return {"table": table, "rows": rows, "total": total,
            "heap": heap, "indexes": indexes, "toast": toast}


def record_stats(lease, m: dict) -> None:
    with lease.cursor() as cur:
        cur.execute("""
            INSERT INTO imdb.ingest_stats
                (table_name, row_count, total_bytes, heap_bytes, index_bytes, toast_bytes, measured_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (table_name) DO UPDATE
               SET row_count = EXCLUDED.row_count,
                   total_bytes = EXCLUDED.total_bytes,
                   heap_bytes  = EXCLUDED.heap_bytes,
                   index_bytes = EXCLUDED.index_bytes,
                   toast_bytes = EXCLUDED.toast_bytes,
                   measured_at = now()
        """, (m["table"], m["rows"], m["total"], m["heap"], m["indexes"], m["toast"]))


class Measurer:
    """
    Measures a table's on-disk footprint the moment it is safely complete:
    the producer has marked it done AND no message for it is still pending or
    processing anywhere. Thread-safe; only worker 0 drives it during the run,
    and main re-runs a full sweep at the end so the numbers are authoritative.
    """

    def __init__(self, args):
        self.args = args
        self._lock = threading.Lock()
        self._measured: set[str] = set()
        self._last_check = 0.0

    def poll(self, lease) -> None:
        now = time.monotonic()
        if now - self._last_check < self.args.measure_seconds:
            return
        self._last_check = now

        for table in completed_tables(lease):
            with self._lock:
                if table in self._measured:
                    continue
            if pending_for_table(lease, self.args.queue, table) == 0:
                self._commit(lease, table)

    def sweep(self, lease, tables: list[str]) -> list[dict]:
        results = []
        for table in tables:
            m = self._commit(lease, table, force=True)
            if m:
                results.append(m)
        return results

    def _commit(self, lease, table: str, force: bool = False) -> dict | None:
        with self._lock:
            if not force and table in self._measured:
                return None
            self._measured.add(table)
        m = measure_table(lease, table, self.args.exact_rows)
        record_stats(lease, m)
        print(f"  [measured] {table:<18} rows={m['rows']:>12,}  "
              f"total={human_bytes(m['total']):>11}  "
              f"(heap {human_bytes(m['heap'])}, idx {human_bytes(m['indexes'])}, "
              f"toast {human_bytes(m['toast'])})", flush=True)
        return m


# =============================================================================
#  Worker
# =============================================================================

def reconnect(conn, autocommit: bool):
    try:
        conn.close()
    except Exception:
        pass
    return connect(autocommit=autocommit)


def should_exit(lease, args, stats: Stats) -> bool:
    """
    A single check that the run *looks* finished. The caller additionally requires
    this to hold across several consecutive polls (drain_grace_polls), which is
    what actually makes it safe.

    Ordering matters. producer_completion() is read BEFORE queue_stats(). The
    producer commits its final batch atomically: the tail 'pending' messages and
    completed=True become visible in the same instant. Reading completion first
    means that if we observe all_completed=True, the producer's commit already
    happened, so the later queue_stats() snapshot is guaranteed to see the tail.
    Reading them the other way round leaves a window where the queue looks empty
    an instant before the tail lands, and the run exits abandoning it.
    """
    if args.follow:
        return False
    all_completed, has_info = producer_completion(lease)   # read FIRST
    st = queue_stats(lease, args.queue)                     # read SECOND
    if st["pending"] > 0 or st["processing"] > 0:
        return False                      # work remains, or another worker is busy
    if has_info:
        return all_completed              # every registered table is done, queue empty
    # No producer info. Exit only after a sustained stretch of emptiness.
    return stats.idle_seconds() >= args.idle_timeout


def worker(worker_id: int, args, stats: Stats, measurer: Measurer) -> None:
    lease = connect(autocommit=True)
    work = connect(autocommit=False)
    # The done-condition must hold on this many consecutive empty polls before the
    # worker exits. One reading can momentarily coincide with the microsecond gap
    # between the producer finishing table N and registering table N+1; requiring
    # the condition to persist for a few seconds steps over that gap.
    done_streak = 0
    try:
        while not SHUTDOWN.is_set():
            try:
                messages = dequeue(lease, args.queue, args.batch, args.visibility)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                print(f"  worker {worker_id}: lease reconnect ({short(exc, 80)})", flush=True)
                lease = reconnect(lease, autocommit=True)
                wait_shutdown(1.0)
                continue

            if not messages:
                if should_exit(lease, args, stats):
                    done_streak += 1
                    if done_streak >= args.drain_grace_polls:
                        break
                else:
                    done_streak = 0
                wait_shutdown(args.poll)
                continue
            done_streak = 0

            stats.mark_active()
            for msg_id, payload in messages:
                if SHUTDOWN.is_set():
                    break
                table, rows = split_payload(payload)
                if table is None or rows is None:
                    nack(lease, msg_id, "malformed payload (no table/data/rows)",
                         args.max_attempts)
                    stats.note_malformed()
                    continue
                try:
                    res = ingest_message(work, table, rows)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    # The insert connection died. The message stays 'processing';
                    # reap_expired() will hand it back. Reconnect and move on.
                    print(f"  worker {worker_id}: work reconnect ({short(exc, 80)})", flush=True)
                    work = reconnect(work, autocommit=False)
                    break
                if res["action"] == "ack":
                    ack(lease, msg_id)
                else:
                    nack(lease, msg_id, res["error"] or "", args.max_attempts)
                stats.record(table, res)

            if worker_id == 0:
                measurer.poll(lease)
    finally:
        try:
            work.close()
        except Exception:
            pass
        try:
            lease.close()
        except Exception:
            pass


# =============================================================================
#  Reaper (its own connection, its own thread)
# =============================================================================

def reaper(args) -> None:
    conn = connect(autocommit=True)
    try:
        while not wait_shutdown(args.reap_seconds):
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT imdb.reap_expired(%s, %s)",
                                (args.queue, args.max_attempts))
                    n = cur.fetchone()[0]
                if n:
                    print(f"  reaper: reclaimed {n} expired lease(s)", flush=True)
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                conn = reconnect(conn, autocommit=True)
            except psycopg2.Error as exc:
                print(f"  reaper: {short(exc, 120)}", flush=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =============================================================================
#  Status line
# =============================================================================

def status_printer(args, stats: Stats) -> None:
    lease = connect(autocommit=True)
    try:
        while not wait_shutdown(args.status_seconds):
            st = queue_stats(lease, args.queue)
            elapsed = time.monotonic() - stats.started
            rate = stats.total_inserted() / elapsed if elapsed else 0.0
            print(f"  [status] msgs={stats.messages:,} rows={stats.total_inserted():,} "
                  f"({rate:,.0f}/s)  queue pending={st['pending']:,} "
                  f"processing={st['processing']:,} done={st['done']:,} failed={st['failed']:,}"
                  f"  bad={stats.bad:,} nacked={stats.nacked:,}", flush=True)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        pass
    finally:
        try:
            lease.close()
        except Exception:
            pass


# =============================================================================
#  DDL for the consumer's private tables
# =============================================================================

CONSUMER_DDL = """
CREATE TABLE IF NOT EXISTS imdb.dead_letter (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    sqlstate   TEXT,
    error      TEXT,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS imdb.ingest_stats (
    table_name  TEXT PRIMARY KEY,
    row_count   BIGINT,
    total_bytes BIGINT,
    heap_bytes  BIGINT,
    index_bytes BIGINT,
    toast_bytes BIGINT,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def write_report(measurements: list[dict]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / "table_sizes.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["table", "rows", "total_bytes", "heap_bytes", "index_bytes", "toast_bytes"])
        for m in measurements:
            w.writerow([m["table"], m["rows"], m["total"], m["heap"], m["indexes"], m["toast"]])
    return path


# =============================================================================
#  Selftest (no database)
# =============================================================================

def selftest() -> int:
    ok = True

    def check(name, cond):
        nonlocal ok
        ok = ok and cond
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")

    t, rows = split_payload({"table": "title_ratings", "data": {"tconst": "tt1"}})
    check("single shape -> one row", t == "title_ratings" and rows == [{"tconst": "tt1"}])

    t, rows = split_payload({"table": "title_basics", "rows": [{"tconst": "tt1"}, {"tconst": "tt2"}]})
    check("batch shape -> row list", t == "title_basics" and len(rows) == 2)

    t, rows = split_payload('{"table": "title_crew", "data": {"tconst": "tt9"}}')
    check("json string is parsed", t == "title_crew" and rows == [{"tconst": "tt9"}])

    t, rows = split_payload({"nonsense": 1})
    check("malformed -> (None, None)", t is None and rows is None)

    v = adapt("title_basics", {"tconst": "tt1", "genres": ["Action", "Drama"],
                               "is_adult": False, "metadata": {"a": 1}})
    check("adapt keeps column order", v[0] == "tt1" and v[4] is False and v[8] == ["Action", "Drama"])
    check("adapt wraps metadata as Json", isinstance(v[9], Json))
    check("adapt leaves NULLs as None", v[1] is None)

    v = adapt("title_principals", {"tconst": "tt1", "ordering": 1, "nconst": "nm1",
                                   "characters": ["Self"]})
    check("adapt wraps characters (jsonb) as Json", isinstance(v[5], Json))
    check("adapt leaves absent metadata None", v[6] is None)

    # is_transient reads .pgcode via getattr; psycopg2.Error.pgcode is read-only,
    # so a lightweight stub with the same attribute is the honest way to test it.
    class E:
        def __init__(self, code):
            self.pgcode = code

    check("40P01 is transient", is_transient(E("40P01")))
    check("40001 is transient", is_transient(E("40001")))
    check("23505 is not transient", not is_transient(E("23505")))
    check("no pgcode is not transient", not is_transient(E(None)))

    for table, spec in TARGETS.items():
        check(f"{table}: INSERT lists every column",
              all(c in INSERTS[table] for c in spec["cols"]) and "ON CONFLICT" in INSERTS[table])

    print("\nselftest PASSED" if ok else "\nselftest FAILED")
    return 0 if ok else 1


# =============================================================================


def main() -> int:
    ap = argparse.ArgumentParser(description="Drain imdb.message_queue into the IMDb tables")
    ap.add_argument("--queue", default=DEFAULT_QUEUE)
    ap.add_argument("--workers", type=int, default=1, help="concurrent drainers in this process")
    ap.add_argument("--batch", type=int, default=20, help="messages claimed per dequeue()")
    ap.add_argument("--visibility", default="5 minutes",
                    help="lease length; must exceed the time to insert one dequeue batch")
    ap.add_argument("--max-attempts", type=int, default=5,
                    help="nack retries before a message is parked in 'failed'")
    ap.add_argument("--poll", type=float, default=1.0, help="sleep when the queue is empty")
    ap.add_argument("--reap-seconds", type=float, default=30.0,
                    help="how often reap_expired() reclaims abandoned leases")
    ap.add_argument("--status-seconds", type=float, default=10.0)
    ap.add_argument("--measure-seconds", type=float, default=15.0,
                    help="how often to check whether a completed table can be measured")
    ap.add_argument("--idle-timeout", type=float, default=30.0,
                    help="exit after this many empty seconds when no producer_progress exists")
    ap.add_argument("--drain-grace-polls", type=int, default=3,
                    help="consecutive empty polls the done-condition must hold before exiting")
    ap.add_argument("--follow", action="store_true",
                    help="never exit on an empty queue; run until Ctrl-C")
    ap.add_argument("--estimate-rows", dest="exact_rows", action="store_false", default=True,
                    help="use pg_class.reltuples instead of an exact count(*) when measuring")
    ap.add_argument("--all", action="store_true",
                    help="also measure title_akas in the final sweep")
    ap.add_argument("--selftest", action="store_true", help="run pure-logic tests and exit")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    signal.signal(signal.SIGINT, _on_sigint)

    setup = connect(autocommit=True)
    with setup.cursor() as cur:
        # A session-level advisory lock serialises the CREATE TABLE IF NOT EXISTS
        # against another consumer starting at the same instant: bare IF NOT EXISTS
        # is racy (it can abort with a duplicate pg_type entry) when two backends
        # create the same table concurrently.
        cur.execute("SELECT pg_advisory_lock(hashtext('imdb.consumer_ddl'))")
        cur.execute(CONSUMER_DDL)
        cur.execute("SELECT pg_advisory_unlock(hashtext('imdb.consumer_ddl'))")
    setup.close()

    stats = Stats()
    measurer = Measurer(args)

    threads: list[threading.Thread] = []
    reaper_t = threading.Thread(target=reaper, args=(args,), name="reaper", daemon=True)
    status_t = threading.Thread(target=status_printer, args=(args, stats), name="status", daemon=True)
    reaper_t.start()
    status_t.start()

    print(f"\nDraining queue {args.queue!r} with {args.workers} worker(s), "
          f"batch={args.batch} messages, visibility={args.visibility}\n", flush=True)

    for i in range(args.workers):
        t = threading.Thread(target=worker, args=(i, args, stats, measurer), name=f"worker-{i}")
        t.start()
        threads.append(t)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        SHUTDOWN.set()
        for t in threads:
            t.join()

    # Final authoritative sweep. Measure every table that received rows (plus akas
    # if asked), even ones whose per-table measurement never fired.
    SHUTDOWN.set()          # stop reaper/status threads
    tables = [d.table for d in (DATASETS if args.all else REQUIRED)]
    final = connect(autocommit=True)
    measured = []
    try:
        for table in tables:
            m = measure_table(final, table, args.exact_rows)
            record_stats(final, m)
            measured.append(m)
        final_stats = queue_stats(final, args.queue)
    finally:
        final.close()

    report = write_report(measured)

    print("\nPer-table footprint")
    grand = 0
    for m in sorted(measured, key=lambda x: x["total"]):
        grand += m["total"]
        print(f"  {m['table']:<18} {m['rows']:>12,} rows  total {human_bytes(m['total']):>11}"
              f"  (heap {human_bytes(m['heap']):>10}, idx {human_bytes(m['indexes']):>10}, "
              f"toast {human_bytes(m['toast']):>9})")
    print(f"  {'TOTAL':<18} {'':>12}       total {human_bytes(grand):>11}")

    elapsed = time.monotonic() - stats.started
    print(f"\nProcessed {stats.messages:,} messages, "
          f"{stats.total_inserted():,} rows inserted "
          f"({sum(stats.skipped.values()):,} skipped as duplicates) in {elapsed:,.0f}s")
    if stats.bad:
        print(f"  {stats.bad:,} row(s) sent to imdb.dead_letter")
    if stats.malformed:
        print(f"  {stats.malformed:,} malformed message(s)")
    print(f"  queue: pending={final_stats['pending']:,} processing={final_stats['processing']:,} "
          f"done={final_stats['done']:,} failed={final_stats['failed']:,}")
    print(f"  size report written to {report}")

    # A clean drain leaves nothing pending/processing/failed and no dead rows.
    # Return non-zero otherwise so a wrapping script can tell the load was imperfect.
    incomplete = (final_stats["pending"] or final_stats["processing"]
                  or final_stats["failed"] or stats.bad or stats.malformed)
    if incomplete:
        print("  WARNING: load is not clean -- see failed/dead_letter counts above.")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
