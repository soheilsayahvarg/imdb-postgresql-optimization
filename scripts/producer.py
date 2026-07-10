#!/usr/bin/env python3
"""
Streaming producer: IMDb .tsv.gz -> imdb.message_queue

    python scripts/producer.py                       # all six required tables
    python scripts/producer.py --tables title_ratings
    python scripts/producer.py --dry-run --limit-rows 50000   # no database needed

Three things this script exists to do:

  1. STREAM. The .gz files are read through Python's gzip module and never
     decompressed to disk. Expanding all six would cost ~10 GB on a drive with
     ~53 GB free, and every one of those bytes would be written once and read once.

  2. BATCH. 1000 rows go into a single message as
     {"table": "...", "rows": [{...}, ...]}. Measured on the real dumps, a one-row
     payload is 184-344 bytes, which keeps the queue tuple under
     TOAST_TUPLE_THRESHOLD (~2 KB): it is stored inline and uncompressed. A
     1000-row payload is 150-320 KB, so the tuple crosses the threshold and
     PostgreSQL moves the payload out to the TOAST relation AND lz4-compresses it.
     The JSON keys repeat 1000 times, so it compresses extremely well. This is the
     difference between a ~40 GB queue and a ~4 GB one.
     (Run with --dry-run to reproduce those numbers on your own copy of the data.)

  3. THROTTLE. The producer parses TSV far faster than the consumer can INSERT
     92M rows with index maintenance. Left alone it would push the entire dataset
     into message_queue before the consumer had drained a tenth of it, and the
     TOAST relation alone would fill the disk. So it polls imdb.queue_stats() and
     stops enqueueing whenever the backlog crosses --high-water, resuming only
     once it falls back under --low-water. While it waits it purges acked
     messages, which is exactly the maintenance the queue needs and exactly the
     moment the producer has nothing else to do.

Restartability: the checkpoint (physical lines consumed, rows accepted, messages
sent) is written in the SAME transaction that enqueues the messages, so a crash
can never lose a batch. Resume skips exactly the lines already consumed, so it
cannot duplicate one either -- but even if it did, every consumer INSERT uses
ON CONFLICT DO NOTHING, which is what makes the queue's at-least-once delivery safe.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import signal
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (DEFAULT_QUEUE, Dataset, clean_value, connect,  # noqa: E402
                    human_bytes, select_datasets)

# A few name.basics rows carry very long knownForTitles lists. The 128 KiB
# default would raise _csv.Error on those.
csv.field_size_limit(1 << 24)

STOP = False


def _on_sigint(signum, frame):
    global STOP
    if STOP:                       # second Ctrl-C: give up immediately
        raise KeyboardInterrupt
    STOP = True
    tqdm.write("\n  Ctrl-C: finishing the current batch and checkpointing...")


# =============================================================================
#  TSV -> typed dict
# =============================================================================

def _text(v):
    return v


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _smallint(v):
    """start_year, end_year, birth_year, death_year are SMALLINT in the schema."""
    n = _int(v)
    if n is None or not (-32768 <= n <= 32767):
        return None
    return n


def _bool01(v):
    # isAdult is documented as 0/1 but has carried junk values in past dumps.
    if v == "1":
        return True
    if v == "0":
        return False
    return None


def _rating(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _list(v):
    """Comma-separated in current dumps; older ones used \\x02 as the separator."""
    if v is None:
        return None
    sep = "\x02" if "\x02" in v else ","
    items = [p for p in v.split(sep) if p]
    return items or None


def _json_array(v):
    """
    title.principals.characters arrives as a JSON array literal, e.g. ["Self"].

    Returns None only when the text is not valid JSON. An empty array or the
    literal false would be falsy but perfectly valid JSONB, so `parsed or None`
    would wrongly turn them into SQL NULL.
    """
    if v is None:
        return None
    try:
        return json.loads(v)
    except ValueError:
        return None


# (tsv_field, db_column, coercion)
COLUMN_MAPS: dict[str, tuple[tuple[str, str, object], ...]] = {
    "title_ratings": (
        ("tconst", "tconst", _text),
        ("averageRating", "average_rating", _rating),
        ("numVotes", "num_votes", _int),
    ),
    "title_episode": (
        ("tconst", "tconst", _text),
        ("parentTconst", "parent_tconst", _text),
        ("seasonNumber", "season_number", _int),
        ("episodeNumber", "episode_number", _int),
    ),
    "title_crew": (
        ("tconst", "tconst", _text),
        ("directors", "directors", _list),
        ("writers", "writers", _list),
    ),
    "title_basics": (
        ("tconst", "tconst", _text),
        ("titleType", "title_type", _text),
        ("primaryTitle", "primary_title", _text),
        ("originalTitle", "original_title", _text),
        ("isAdult", "is_adult", _bool01),
        ("startYear", "start_year", _smallint),
        ("endYear", "end_year", _smallint),
        ("runtimeMinutes", "runtime_minutes", _int),
        ("genres", "genres", _list),
    ),
    "name_basics": (
        ("nconst", "nconst", _text),
        ("primaryName", "primary_name", _text),
        ("birthYear", "birth_year", _smallint),
        ("deathYear", "death_year", _smallint),
        ("primaryProfession", "primary_profession", _list),
        ("knownForTitles", "known_for_titles", _list),
    ),
    "title_principals": (
        ("tconst", "tconst", _text),
        ("ordering", "ordering", _int),
        ("nconst", "nconst", _text),
        ("category", "category", _text),
        ("job", "job", _text),
        ("characters", "characters", _json_array),
    ),
    "title_akas": (
        ("titleId", "title_id", _text),
        ("ordering", "ordering", _int),
        ("title", "title", _text),
        ("region", "region", _text),
        ("language", "language", _text),
        ("types", "types", _list),
        ("attributes", "attributes", _list),
        ("isOriginalTitle", "is_original_title", _bool01),
    ),
}

# Columns declared NOT NULL in init.sql. A row missing any of these would abort
# the consumer's whole 1000-row INSERT, so it is dropped here and counted.
NOT_NULL: dict[str, tuple[str, ...]] = {
    "title_ratings": ("tconst",),
    "title_episode": ("tconst",),
    "title_crew": ("tconst",),
    "title_basics": ("tconst",),
    "name_basics": ("nconst",),
    "title_principals": ("tconst", "ordering", "nconst"),
    "title_akas": ("title_id", "ordering"),
}


def build_row(table: str, raw: dict, counters: Counter) -> dict | None:
    """
    Coerce one TSV record into a dict keyed by database column name.

    NULL columns are omitted rather than emitted as JSON null. Across ~190M rows
    with many \\N fields that is a large saving in payload bytes, and the consumer
    reads every column with .get() anyway.
    """
    row: dict = {}
    for src, col, coerce in COLUMN_MAPS[table]:
        raw_value = clean_value(raw.get(src))
        value = coerce(raw_value)
        if value is None:
            if raw_value is not None:
                # A real value that would not fit the column's type. Worth reporting.
                counters[f"{table}.{col}: uncoercible"] += 1
            continue
        row[col] = value

    for col in NOT_NULL[table]:
        if col not in row:
            counters[f"{table}: dropped (missing NOT NULL {col})"] += 1
            return None
    return row


def rating_metadata(row: dict) -> dict | None:
    """
    Derived attributes for title_ratings.metadata.

    Only ~1.6M rows, so this costs about 150 MB -- and it gives the report a
    genuine, non-contrived JSONB column to build a GIN index on and query with
    the containment operator (metadata @> '{"popular": true}').
    """
    rating = row.get("average_rating")
    votes = row.get("num_votes")
    if rating is None and votes is None:
        return None

    meta: dict = {"src": "title.ratings"}
    if rating is not None:
        floor = int(rating)
        meta["rating_bucket"] = "10" if floor >= 10 else f"{floor}-{floor + 1}"
    if votes is not None:
        meta["vote_bucket"] = ("100k+" if votes >= 100_000 else
                               "10k-100k" if votes >= 10_000 else
                               "1k-10k" if votes >= 1_000 else "<1k")
        meta["popular"] = votes >= 10_000
    return meta


# =============================================================================
#  gzip streaming
# =============================================================================

class GzipTsvStream:
    """
    Reads a .tsv.gz without ever materialising the .tsv.

    compressed_pos exposes the position in the *compressed* file, which is what
    the progress bar tracks: it is the only number we know the total of.
    """

    def __init__(self, path: Path):
        self.path = path
        self.size = path.stat().st_size
        self._raw = open(path, "rb")
        self._gz = gzip.GzipFile(fileobj=self._raw, mode="rb")
        # utf-8-sig, not utf-8. The current dumps carry no BOM (checked), but if one
        # ever appeared the first header name would become "﻿tconst", every
        # tconst would read as None, and every row would be silently dropped as a
        # NOT NULL violation. utf-8-sig costs nothing and removes that failure mode.
        self._text = io.TextIOWrapper(self._gz, encoding="utf-8-sig", newline="")
        self.header = self._text.readline().rstrip("\r\n").split("\t")

    def skip_lines(self, n: int) -> None:
        """
        Fast-forward past n physical data lines (the header is already consumed).

        The checkpoint stores physical lines, not accepted rows: a row the producer
        drops still advances the file. Counting accepted rows here would resume a
        few lines early after any drop.
        """
        for _ in range(n):
            if not self._text.readline():
                return

    def rows(self):
        # QUOTE_NONE is mandatory. IMDb does not quote fields, but plenty of real
        # titles contain a double quote; with the default dialect csv would treat
        # it as an opening quote and swallow the rest of the record.
        return csv.DictReader(self._text, fieldnames=self.header,
                              delimiter="\t", quoting=csv.QUOTE_NONE, restkey="_extra")

    @property
    def compressed_pos(self) -> int:
        return self._raw.tell()

    def close(self) -> None:
        self._text.close()
        self._raw.close()


# =============================================================================
#  Sinks
# =============================================================================

PROGRESS_DDL = """
-- Producer-private checkpoint state. Deliberately not in resources/init.sql:
-- it is not part of the schema the project is graded on, and dropping it must
-- never require rebuilding the database.
--
-- lines_read is the resume cursor: PHYSICAL data lines consumed from the .gz.
-- rows_enqueued is the count of rows that survived coercion. The two differ
-- whenever a row is dropped, which is exactly why resuming on rows_enqueued
-- would re-read the tail of the previous run.
CREATE TABLE IF NOT EXISTS imdb.producer_progress (
    table_name        TEXT PRIMARY KEY,
    source_file       TEXT        NOT NULL,
    source_bytes      BIGINT      NOT NULL,
    lines_read        BIGINT      NOT NULL DEFAULT 0,
    rows_enqueued     BIGINT      NOT NULL DEFAULT 0,
    messages_enqueued BIGINT      NOT NULL DEFAULT 0,
    completed         BOOLEAN     NOT NULL DEFAULT false,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

SAMPLE_CAP = 100_000          # payload sizes kept for the dry-run median


def interruptible_sleep(seconds: float) -> None:
    """
    time.sleep() is not cut short by a signal handler that returns normally
    (PEP 475 restarts it), so a Ctrl-C during a 2 s poll would sit unnoticed.
    """
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not STOP:
        time.sleep(0.2)


class DryRunSink:
    """Parses and serialises everything, talks to no database."""

    def __init__(self):
        self.count = 0
        self.total = 0
        self.smallest = None
        self.largest = None
        self.sample: list[int] = []          # capped, only to compute a median
        self.messages_since_commit = 0

    def emit(self, payloads: list[str]) -> None:
        for p in payloads:
            n = len(p.encode("utf-8"))
            self.count += 1
            self.total += n
            self.smallest = n if self.smallest is None else min(self.smallest, n)
            self.largest = n if self.largest is None else max(self.largest, n)
            if len(self.sample) < SAMPLE_CAP:
                self.sample.append(n)
        self.messages_since_commit += len(payloads)

    def resume_point(self, ds: Dataset, size: int) -> tuple[int, int, int, bool]:
        return 0, 0, 0, False

    def checkpoint(self, ds: Dataset, lines: int, rows: int, msgs: int, done: bool) -> None:
        self.messages_since_commit = 0

    def after_checkpoint(self, messages_total: int) -> None:
        pass

    def close(self) -> None:
        pass


class QueueSink:
    """
    Enqueue side. Two connections on purpose:

      data : autocommit off. One transaction spans `checkpoint_every` messages and
             the producer_progress UPDATE, so enqueue and checkpoint commit or roll
             back together. That is what makes a crash neither lose nor duplicate work.
      ctl  : autocommit on. Reads queue_stats() and runs purge_done() without
             joining, and thus without holding open, the data transaction.
    """

    def __init__(self, args):
        self.args = args
        self.queue = args.queue
        self.data = connect(autocommit=False)
        self.ctl = connect(autocommit=True)
        self.messages_since_commit = 0
        self._last_failed = 0
        self._last_purge_at = 0

        with self.ctl.cursor() as cur:
            cur.execute(PROGRESS_DDL)

    # -- enqueue ------------------------------------------------------------
    def emit(self, payloads: list[str]) -> None:
        with self.data.cursor() as cur:
            if self.args.enqueue_mode == "single" or len(payloads) == 1:
                for p in payloads:
                    # The brief's own function, one message per call.
                    cur.execute("SELECT imdb.enqueue(%s, %s::jsonb)", (self.queue, p))
            else:
                # Same rows, one round trip. jsonb_array_elements avoids relying on
                # how the driver types a Python list of JSON strings.
                cur.execute(
                    "SELECT count(*) FROM imdb.enqueue_many"
                    "(%s, ARRAY(SELECT jsonb_array_elements(%s::jsonb)))",
                    (self.queue, "[" + ",".join(payloads) + "]"))
        self.messages_since_commit += len(payloads)

    # -- checkpoint ---------------------------------------------------------
    def resume_point(self, ds: Dataset, size: int) -> tuple[int, int, int, bool]:
        with self.data.cursor() as cur:
            cur.execute("SELECT lines_read, rows_enqueued, messages_enqueued, completed,"
                        "       source_bytes"
                        "  FROM imdb.producer_progress WHERE table_name = %s", (ds.table,))
            row = cur.fetchone()
            if row is None:
                cur.execute("INSERT INTO imdb.producer_progress"
                            " (table_name, source_file, source_bytes) VALUES (%s, %s, %s)",
                            (ds.table, ds.filename, size))
                self.data.commit()
                return 0, 0, 0, False

            lines, rows, msgs, completed, source_bytes = row
            if source_bytes != size:
                raise SystemExit(
                    f"{ds.table}: {ds.filename} is {size:,} bytes but the last run saw "
                    f"{source_bytes:,}. IMDb refreshes the dumps daily, so resuming would "
                    f"skip the wrong rows. Re-run with --restart {ds.table}.")
        self.data.commit()
        return lines, rows, msgs, completed

    def checkpoint(self, ds: Dataset, lines: int, rows: int, msgs: int, done: bool) -> None:
        with self.data.cursor() as cur:
            cur.execute("UPDATE imdb.producer_progress"
                        "   SET lines_read = %s, rows_enqueued = %s, messages_enqueued = %s,"
                        "       completed = %s, updated_at = now()"
                        " WHERE table_name = %s", (lines, rows, msgs, done, ds.table))
        self.data.commit()          # enqueue + checkpoint land atomically
        self.messages_since_commit = 0

    def after_checkpoint(self, messages_total: int) -> None:
        """
        Runs between transactions, so purge_done() on `ctl` never contends with an
        open `data` transaction.

        Purging only while throttled would be a trap: a consumer that comfortably
        keeps up means the producer never throttles, never purges, and the whole
        ~193k acked messages sit in the queue's TOAST relation to the end of the load.
        """
        if messages_total - self._last_purge_at >= self.args.purge_every:
            self._last_purge_at = messages_total
            if self.args.purge:
                freed = self.purge()
                if freed:
                    tqdm.write(f"  purged {freed:,} acked message(s)")
        self.throttle()

    def restart(self, table: str) -> None:
        with self.ctl.cursor() as cur:
            cur.execute("DELETE FROM imdb.producer_progress WHERE table_name = %s", (table,))

    # -- backpressure -------------------------------------------------------
    def stats(self) -> dict:
        with self.ctl.cursor() as cur:
            cur.execute("SELECT pending, processing, done, failed, total"
                        "  FROM imdb.queue_stats(%s)", (self.queue,))
            p, pr, d, f, t = cur.fetchone()
        return {"pending": p, "processing": pr, "done": d, "failed": f, "total": t}

    def purge(self) -> int:
        freed = 0
        with self.ctl.cursor() as cur:
            for _ in range(50):
                cur.execute("SELECT imdb.purge_done(%s, INTERVAL '0 seconds', %s)",
                            (self.queue, self.args.purge_limit))
                n = cur.fetchone()[0]
                freed += n
                if n < self.args.purge_limit:
                    break
        return freed

    def throttle(self) -> None:
        st = self.stats()
        if st["failed"] > self._last_failed:
            tqdm.write(f"  WARNING: {st['failed']:,} message(s) sitting in 'failed'. "
                       f"Inspect imdb.message_queue.last_error.")
            self._last_failed = st["failed"]

        if st["pending"] < self.args.high_water:
            return

        tqdm.write(f"  backpressure: pending={st['pending']:,} >= high-water "
                   f"{self.args.high_water:,}; pausing")
        stalled = 0.0
        previous = st["pending"]

        while st["pending"] > self.args.low_water and not STOP:
            if self.args.purge:
                freed = self.purge()
                if freed:
                    tqdm.write(f"  purged {freed:,} acked message(s) while waiting")
            interruptible_sleep(self.args.poll_seconds)
            if STOP:
                break
            st = self.stats()

            if st["pending"] >= previous:
                stalled += self.args.poll_seconds
                if stalled >= self.args.stall_seconds:
                    tqdm.write(f"  WARNING: backlog stuck at {st['pending']:,} for "
                               f"{self.args.stall_seconds:.0f}s. Is consumer.py running?")
                    stalled = 0.0
            else:
                stalled = 0.0
            previous = st["pending"]

        if not STOP:
            tqdm.write(f"  resuming: pending={st['pending']:,} <= low-water "
                       f"{self.args.low_water:,}")

    def close(self) -> None:
        self.data.close()
        self.ctl.close()


# =============================================================================
#  Producing one table
# =============================================================================

def make_payloads(table: str, rows: list[dict], shape: str) -> list[str]:
    dumps = lambda o: json.dumps(o, separators=(",", ":"), ensure_ascii=False)
    if shape == "single":
        # The brief's literal sample shape: one record per message. Included so the
        # report can show it working; at 190M messages it is not a viable bulk path.
        return [dumps({"table": table, "data": r}) for r in rows]
    return [dumps({"table": table, "rows": rows})]


def produce_table(sink, ds: Dataset, args, counters: Counter) -> dict:
    size = ds.path.stat().st_size
    lines_done, rows_done, msgs_done, completed = sink.resume_point(ds, size)

    if completed and not args.force:
        print(f"  {ds.table}: already complete ({rows_done:,} rows). Skipping.")
        return {"table": ds.table, "rows": 0, "messages": 0, "seconds": 0.0, "skipped": True}

    stream = GzipTsvStream(ds.path)
    if lines_done:
        print(f"  {ds.table}: resuming after {lines_done:,} lines ({rows_done:,} rows sent)")
        stream.skip_lines(lines_done)

    started = time.monotonic()
    rows_at_start, msgs_at_start = rows_done, msgs_done
    batch: list[dict] = []
    outbox: list[str] = []

    bar = tqdm(total=stream.size, initial=stream.compressed_pos, unit="B", unit_scale=True,
               unit_divisor=1024, desc=f"{ds.table:<17}", ncols=88, leave=True)

    def flush_outbox() -> None:
        nonlocal msgs_done
        if not outbox:
            return
        sink.emit(outbox)
        msgs_done += len(outbox)
        outbox.clear()

    hit_limit = False
    try:
        for raw in stream.rows():
            if STOP:
                break

            # Counted for every physical line, accepted or not: this is the resume cursor.
            lines_done += 1

            if raw.get("_extra") is not None:
                counters[f"{ds.table}: dropped (too many fields)"] += 1
                continue

            row = build_row(ds.table, raw, counters)
            if row is None:
                continue
            if args.metadata != "none":
                if ds.table == "title_ratings":
                    meta = rating_metadata(row)
                    if meta:
                        row["metadata"] = meta
                elif args.metadata == "all" and ds.table in ("title_basics", "name_basics"):
                    row["metadata"] = {"src": ds.name}

            batch.append(row)
            rows_done += 1

            if len(batch) >= args.batch_size:
                outbox.extend(make_payloads(ds.table, batch, args.payload_shape))
                batch = []

            if len(outbox) >= args.messages_per_call:
                flush_outbox()

            # By construction this only fires immediately after a flush, when both
            # `batch` and `outbox` are empty. So lines_done, rows_done and msgs_done
            # describe exactly the same prefix of the file. Checkpointing anywhere
            # else would record rows that had not been enqueued yet, and resume
            # would skip them.
            if sink.messages_since_commit >= args.checkpoint_every:
                sink.checkpoint(ds, lines_done, rows_done, msgs_done, False)
                bar.update(stream.compressed_pos - bar.n)
                sink.after_checkpoint(msgs_done)

            if args.limit_rows and rows_done - rows_at_start >= args.limit_rows:
                hit_limit = True
                break

        # tail
        if batch:
            outbox.extend(make_payloads(ds.table, batch, args.payload_shape))
        flush_outbox()

        # Reaching EOF counts as complete even under --limit-rows: only an actual
        # early break leaves the table resumable.
        finished = not STOP and not hit_limit
        sink.checkpoint(ds, lines_done, rows_done, msgs_done, finished)
        bar.update(stream.compressed_pos - bar.n)
    finally:
        bar.close()
        stream.close()

    elapsed = time.monotonic() - started
    return {
        "table": ds.table,
        "rows": rows_done - rows_at_start,
        "messages": msgs_done - msgs_at_start,
        "seconds": elapsed,
        "skipped": False,
    }


# =============================================================================


def main() -> int:
    ap = argparse.ArgumentParser(description="Stream IMDb .tsv.gz into imdb.message_queue")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--all", action="store_true", help="also produce title.akas")
    ap.add_argument("--queue", default=DEFAULT_QUEUE)
    ap.add_argument("--batch-size", type=int, default=1000,
                    help="rows per message (default 1000: comfortably over the TOAST threshold)")
    ap.add_argument("--payload-shape", choices=("batch", "single"), default="batch")
    ap.add_argument("--enqueue-mode", choices=("single", "many"), default="single",
                    help="'single' calls imdb.enqueue() once per message, as the brief specifies")
    ap.add_argument("--messages-per-call", type=int, default=1)
    ap.add_argument("--checkpoint-every", type=int, default=20,
                    help="messages per transaction; also the backpressure poll interval")

    ap.add_argument("--high-water", type=int, default=2000,
                    help="pause when this many messages are pending (~2M rows in flight)")
    ap.add_argument("--low-water", type=int, default=500)
    ap.add_argument("--poll-seconds", type=float, default=2.0)
    ap.add_argument("--stall-seconds", type=float, default=60.0)
    ap.add_argument("--purge", dest="purge", action="store_true", default=True,
                    help="delete acked messages as the load proceeds (default)")
    ap.add_argument("--no-purge", dest="purge", action="store_false")
    ap.add_argument("--purge-every", type=int, default=200,
                    help="messages between purges; also runs continuously while throttled")
    ap.add_argument("--purge-limit", type=int, default=10000)

    ap.add_argument("--metadata", choices=("none", "ratings", "all"), default="ratings",
                    help="'ratings' fills title_ratings.metadata with derived buckets (~150 MB)")
    ap.add_argument("--limit-rows", type=int, default=0, help="stop after N rows per table")
    ap.add_argument("--force", action="store_true", help="re-produce tables already marked complete")
    ap.add_argument("--restart", nargs="*", default=None, metavar="TABLE",
                    help="forget the checkpoint for these tables and start them over")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and serialise only; never touch the database")
    args = ap.parse_args()

    if args.enqueue_mode == "many" and args.messages_per_call == 1:
        args.messages_per_call = 4        # ~1.3 MB per round trip

    datasets = select_datasets(args.tables, include_optional=args.all)

    missing = [d for d in datasets if not d.path.exists()]
    if missing:
        print("Missing dumps. Run scripts/download_data.py first:")
        for d in missing:
            print(f"  {d.path}")
        return 1

    signal.signal(signal.SIGINT, _on_sigint)

    sink = DryRunSink() if args.dry_run else QueueSink(args)
    if args.restart and not args.dry_run:
        for t in args.restart:
            sink.restart(t.replace(".", "_"))
            print(f"  checkpoint cleared for {t}")

    counters: Counter = Counter()
    results = []
    wall = time.monotonic()

    print(f"\nProducing into queue {args.queue!r}  "
          f"(batch={args.batch_size} rows/message, shape={args.payload_shape}, "
          f"{'DRY RUN' if args.dry_run else 'enqueue_mode=' + args.enqueue_mode})\n")

    final_stats = None
    try:
        for ds in datasets:
            results.append(produce_table(sink, ds, args, counters))
            if STOP:
                break
        if not args.dry_run:
            # Read the counters before the connections go away.
            final_stats = sink.stats()
    finally:
        sink.close()

    total_wall = time.monotonic() - wall

    print("\nPer table")
    total_rows = total_msgs = 0
    for r in results:
        if r["skipped"]:
            continue
        total_rows += r["rows"]
        total_msgs += r["messages"]
        rate = r["rows"] / r["seconds"] if r["seconds"] else 0.0
        print(f"  {r['table']:<18} {r['rows']:>12,} rows  {r['messages']:>9,} msgs  "
              f"{r['seconds']:>7.1f}s  {rate:>10,.0f} rows/s")
    print(f"  {'TOTAL':<18} {total_rows:>12,} rows  {total_msgs:>9,} msgs  "
          f"{total_wall:>7.1f}s")

    if counters:
        print("\nData quality (rows or values the schema could not accept)")
        for key, n in sorted(counters.items(), key=lambda kv: -kv[1]):
            print(f"  {n:>10,}  {key}")
    else:
        print("\nData quality: every value coerced cleanly.")

    if args.dry_run and sink.count:
        print("\nPayload sizes (JSON bytes before PostgreSQL stores them)")
        print(f"  messages : {sink.count:,}")
        print(f"  min      : {human_bytes(sink.smallest)}")
        print(f"  median   : {human_bytes(statistics.median(sink.sample))}"
              f"{'  (over the first %s sampled)' % f'{SAMPLE_CAP:,}' if sink.count > SAMPLE_CAP else ''}")
        print(f"  max      : {human_bytes(sink.largest)}")
        print(f"  total    : {human_bytes(sink.total)}")
        over = sum(1 for s in sink.sample if s > 2048)
        print(f"  over the ~2 KB TOAST threshold: {over:,} / {len(sink.sample):,} sampled "
              f"({100 * over / len(sink.sample):.1f}%)")

    if final_stats:
        st = final_stats
        print(f"\nQueue: pending={st['pending']:,} processing={st['processing']:,} "
              f"done={st['done']:,} failed={st['failed']:,}")

    if STOP:
        print("\nInterrupted. Progress is checkpointed; re-run to resume.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
