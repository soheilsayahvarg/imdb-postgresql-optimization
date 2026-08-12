# IMDb PostgreSQL Optimization

A PostgreSQL 16 project built around the ~190M-row [IMDb non-commercial dataset](https://datasets.imdbws.com/). It streams the raw `.tsv.gz` dumps through a durable, `SKIP LOCKED`-based message queue into the database, then benchmarks and indexes eight analytical query scenarios with real `EXPLAIN (ANALYZE, BUFFERS)` measurements.

## What's in here

- **A message queue built on a plain table.** `enqueue` / `dequeue` / `ack` / `nack` / `reap_expired` as PL/pgSQL functions, using `SELECT ... FOR UPDATE SKIP LOCKED` so multiple consumers can drain the queue concurrently without blocking each other.
- **A streaming producer/consumer pipeline** (`scripts/producer.py`, `scripts/consumer.py`) that reads the compressed IMDb dumps without ever extracting them to disk, batches 1000 rows per message (crossing PostgreSQL's TOAST threshold on purpose, for real compression), and bulk-inserts with `execute_values` inside per-batch `SAVEPOINT`s, falling back to dead-lettering only the rows that actually fail.
- **Eight analytical query scenarios** (genre ratings, top-rated titles, prolific directors, yearly trends, multi-genre actors, longest-running series, genre-pair popularity, longest movies per genre), each measured before and after indexing.
- **Six targeted indexes** (including a composite GIN index combining a scalar column with an array column via `btree_gin`), with real before/after timing, buffer, and query-plan comparisons -- including one case where an index made a query *slower*, reported and explained rather than hidden.
- **A full write-up** (`report/`, compiled to `report/report.pdf`) covering PostgreSQL configuration tuning, index types, JSONB internals and `TOAST`, and message-queue theory.

## Stack

PostgreSQL 16 - Docker Compose - Python (`psycopg2`) - `pgstattuple` / `btree_gin` - LaTeX (XeLaTeX + `xepersian`)

## Layout

```
resources/     docker-compose.yml, schema (init.sql)
sql/           queue functions, indexes, query scenarios, EXPLAIN scripts
scripts/       producer, consumer, scenario runner, timing/plot helpers
results/       captured timings, EXPLAIN ANALYZE output, comparison table
report/        LaTeX source and the compiled report
```

## Running it

```bash
cd resources && docker compose up -d
python scripts/download_data.py
python scripts/producer.py
python scripts/consumer.py
python scripts/run_scenarios.py --phase before
docker exec -i imdb_postgres psql -U imdb -d imdb -f - < sql/03_indexes.sql
python scripts/run_scenarios.py --phase after
python scripts/compare_timings.py
```

## Results, honestly

Measured on a disk-constrained machine, so `title_principals` (the largest table, ~90M rows) was loaded at ~9% of its full size; every other table is complete. Of the eight scenarios, six got measurably faster after indexing, one stayed effectively the same by design (no index was ever meant to help it), and one got slower -- a real regression caused by a non-selective filter picking a `Bitmap Heap Scan` over a cheaper sequential scan, reported in the write-up along with the `EXPLAIN` evidence for why. Full numbers, plans, and analysis are in `report/report.pdf`.
