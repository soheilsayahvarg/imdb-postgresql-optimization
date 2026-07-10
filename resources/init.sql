-- =============================================================================
--  IMDb / PostgreSQL optimisation project -- initial schema
--
--  Executed exactly once, by the postgres image entrypoint, on the first
--  `docker compose up -d` (i.e. while the data directory is still empty).
--  Re-running it against a populated volume is a no-op only where guarded by
--  IF NOT EXISTS; to start over, run `docker compose down -v`.
--
--  Scope of this file: schema, extensions, base tables, the message queue.
--  Deliberately NOT here:
--    * foreign keys       -> see sql/02_constraints.sql (added after ingestion)
--    * scenario indexes   -> see sql/03_indexes.sql     (added in the tuning phase)
--    * queue functions    -> see sql/01_queue_functions.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- -----------------------------------------------------------------------------
--  Extensions
-- -----------------------------------------------------------------------------

-- Per-statement execution statistics. Requires pg_stat_statements in
-- shared_preload_libraries, which docker-compose.yml sets on the command line.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Lets a single GIN index mix an array/jsonb column with plain scalar columns
-- (e.g. genres + title_type). Used in the index-tuning phase.
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Reports real on-disk tuple and TOAST statistics. Used to measure the actual
-- compression ratio of the batched JSONB payloads rather than estimating it.
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- -----------------------------------------------------------------------------
--  Schema
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS imdb AUTHORIZATION CURRENT_USER;

-- The database name comes from POSTGRES_DB, so resolve it at runtime.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET search_path TO imdb, public',
                   current_database());
END
$$;

SET search_path TO imdb, public;

-- =============================================================================
--  IMDb base tables
--
--  Column names are snake_case renderings of the camelCase TSV headers, which
--  were read directly from the live files at https://datasets.imdbws.com/.
--
--  Comma-separated TSV fields (genres, directors, writers, primaryProfession,
--  knownForTitles, types, attributes) are stored as TEXT[]. The `genres` array
--  in particular is required by the project brief, whose sample query does
--  `CROSS JOIN LATERAL unnest(tb.genres)`.
--
--  Every table carries a nullable `metadata JSONB` column. It stays NULL on the
--  two giant tables (title_principals, title_akas), where it costs one bit in the
--  null bitmap rather than a stored datum. On title_principals (7 columns) the
--  bitmap is 1 byte and t_hoff stays at MAXALIGN(23+1) = 24, so the column really
--  is free there. On a wider table the bitmap can push t_hoff up by 8 bytes, but
--  those rows carry real NULLs (region, language) regardless.
--
--  No foreign keys are declared here, on purpose. Messages are dequeued out of
--  order, so a title_ratings row can legitimately arrive before its
--  title_basics parent. FKs would abort those inserts, and they would also add
--  a per-row index probe to a ~190M-row bulk load. They are added afterwards,
--  as NOT VALID and then VALIDATE CONSTRAINT, in sql/02_constraints.sql.
-- =============================================================================

-- -----------------------------------------------------------------------------
--  title.basics.tsv.gz
--  tconst | titleType | primaryTitle | originalTitle | isAdult | startYear
--         | endYear | runtimeMinutes | genres
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_basics (
    tconst          TEXT     NOT NULL,
    title_type      TEXT,
    primary_title   TEXT,
    original_title  TEXT,
    is_adult        BOOLEAN,
    start_year      SMALLINT,
    end_year        SMALLINT,
    runtime_minutes INTEGER,
    genres          TEXT[],
    metadata        JSONB,
    CONSTRAINT title_basics_pkey PRIMARY KEY (tconst)
);

-- -----------------------------------------------------------------------------
--  title.ratings.tsv.gz
--  tconst | averageRating | numVotes
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_ratings (
    tconst         TEXT         NOT NULL,
    -- NUMERIC, not REAL: AVG() over a float column is not reproducible across
    -- plans, because parallel aggregation changes the summation order.
    average_rating NUMERIC(3,1),
    num_votes      INTEGER,
    metadata       JSONB,
    CONSTRAINT title_ratings_pkey PRIMARY KEY (tconst)
);

-- -----------------------------------------------------------------------------
--  title.crew.tsv.gz
--  tconst | directors | writers
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_crew (
    tconst    TEXT NOT NULL,
    directors TEXT[],
    writers   TEXT[],
    metadata  JSONB,
    CONSTRAINT title_crew_pkey PRIMARY KEY (tconst)
);

-- -----------------------------------------------------------------------------
--  title.episode.tsv.gz
--  tconst | parentTconst | seasonNumber | episodeNumber
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_episode (
    tconst         TEXT    NOT NULL,
    parent_tconst  TEXT,
    season_number  INTEGER,
    episode_number INTEGER,
    metadata       JSONB,
    CONSTRAINT title_episode_pkey PRIMARY KEY (tconst)
);

-- -----------------------------------------------------------------------------
--  name.basics.tsv.gz
--  nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.name_basics (
    nconst             TEXT     NOT NULL,
    primary_name       TEXT,
    birth_year         SMALLINT,
    death_year         SMALLINT,
    primary_profession TEXT[],
    known_for_titles   TEXT[],
    metadata           JSONB,
    CONSTRAINT name_basics_pkey PRIMARY KEY (nconst)
);

-- -----------------------------------------------------------------------------
--  title.principals.tsv.gz  -- ~90M rows, the largest table in the project
--  tconst | ordering | nconst | category | job | characters
--
--  `ordering` is declared first on purpose. Attributes are laid out in
--  declaration order. Short text values (<= 126 bytes) use a 1-byte varlena
--  header and are stored unaligned, but a 4-byte int placed *after* them still
--  needs its own 4-byte alignment, costing 0-3 bytes of padding per row.
--  Declaring it first drops that to zero: on the order of 150 MB across 90M rows.
--
--  `characters` arrives as a JSON array literal, e.g. ["Self"], so it is stored
--  as JSONB rather than TEXT.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_principals (
    ordering   INTEGER NOT NULL,
    tconst     TEXT    NOT NULL,
    nconst     TEXT    NOT NULL,
    category   TEXT,
    job        TEXT,
    characters JSONB,
    metadata   JSONB,
    CONSTRAINT title_principals_pkey PRIMARY KEY (tconst, ordering)
);

-- -----------------------------------------------------------------------------
--  title.akas.tsv.gz  -- ~51M rows
--  titleId | ordering | title | region | language | types | attributes | isOriginalTitle
--
--  Note: none of the eight report scenarios read this table. It is loaded for
--  completeness and is the first thing to drop if the ~53 GB of free disk runs short.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imdb.title_akas (
    ordering          INTEGER NOT NULL,   -- fixed-width first, see title_principals
    is_original_title BOOLEAN,
    title_id          TEXT    NOT NULL,
    title             TEXT,
    region            TEXT,
    language          TEXT,
    types             TEXT[],
    attributes        TEXT[],
    metadata          JSONB,
    CONSTRAINT title_akas_pkey PRIMARY KEY (title_id, ordering)
);

-- =============================================================================
--  Message queue
--
--  A single table acting as a durable work queue. Consumers claim rows with
--  SELECT ... FOR UPDATE SKIP LOCKED, which lets N workers grab N disjoint
--  batches concurrently instead of serialising on the same head-of-queue row.
--
--  `payload` holds either of two shapes:
--    single : {"table": "title_basics", "data": {...}}          <- brief's sample
--    batch  : {"table": "title_basics", "rows": [{...}, ...]}   <- bulk ingestion
--
--  The batch shape is what makes the full dataset fit. TOAST is triggered per
--  TUPLE, not per attribute: only once a whole row exceeds TOAST_TUPLE_THRESHOLD
--  (~2 KB) does PostgreSQL start compressing and out-lining its widest varlena
--  attributes. A single-row payload is ~200 bytes, so the tuple stays under the
--  threshold and the JSONB is kept inline and *uncompressed* -- 190M such rows
--  would be tens of gigabytes. A 1000-row payload is a few hundred KB, so the
--  tuple blows past the threshold and the payload is lz4-compressed and pushed
--  out to the TOAST relation, where the endlessly repeated JSON keys compress
--  extremely well. The per-row 24-byte header overhead collapses by 1000x too.
-- =============================================================================

CREATE TABLE IF NOT EXISTS imdb.message_queue (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    queue_name   TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',
    payload      JSONB       NOT NULL,
    last_error   TEXT,
    -- Set by dequeue() to now() + p_visibility_timeout. A message whose lease
    -- expires while still 'processing' is reclaimable: this is what makes the
    -- queue survive a consumer that crashes mid-batch.
    locked_until TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT message_queue_pkey PRIMARY KEY (id),
    CONSTRAINT message_queue_status_chk
        CHECK (status IN ('pending', 'processing', 'done', 'failed'))
);

-- Partial index: the planner only ever needs to find claimable rows, and once a
-- message reaches 'done' it drops out of the index entirely. This keeps the
-- index at the size of the *backlog*, not of the whole queue history.
CREATE INDEX IF NOT EXISTS message_queue_ready_idx
    ON imdb.message_queue (queue_name, id)
    WHERE status = 'pending';

-- Used by the reaper that returns expired leases to 'pending'.
CREATE INDEX IF NOT EXISTS message_queue_stuck_idx
    ON imdb.message_queue (locked_until)
    WHERE status = 'processing';

-- EXTENDED is already jsonb's default storage; stated explicitly because the
-- out-of-line + compressed behaviour is load-bearing for this design.
ALTER TABLE imdb.message_queue ALTER COLUMN payload SET STORAGE EXTENDED;
ALTER TABLE imdb.message_queue ALTER COLUMN payload SET COMPRESSION lz4;

ALTER TABLE imdb.message_queue SET (
    -- Every message is UPDATEd at least twice (claim, then ack). Leaving free
    -- space on each page lets those updates stay on-page.
    fillfactor = 80,

    -- A queue produces dead tuples far faster than the default
    -- 20%-of-table threshold reacts to. Vacuum on an absolute row count instead.
    autovacuum_enabled              = true,
    autovacuum_vacuum_scale_factor  = 0.0,
    autovacuum_vacuum_threshold     = 2000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold    = 2000,
    autovacuum_vacuum_cost_delay    = 0,

    -- The TOAST side holds the compressed batch payloads and churns just as hard.
    toast.autovacuum_vacuum_scale_factor = 0.0,
    toast.autovacuum_vacuum_threshold    = 2000,
    toast.autovacuum_vacuum_cost_delay   = 0
);

-- =============================================================================
--  Documentation
-- =============================================================================

COMMENT ON SCHEMA imdb IS
    'IMDb non-commercial dataset plus a SKIP LOCKED message queue.';

COMMENT ON TABLE  imdb.title_basics     IS 'One row per title. Source: title.basics.tsv.gz';
COMMENT ON TABLE  imdb.title_ratings    IS 'Aggregate user ratings. Source: title.ratings.tsv.gz';
COMMENT ON TABLE  imdb.title_crew       IS 'Directors and writers per title. Source: title.crew.tsv.gz';
COMMENT ON TABLE  imdb.title_episode    IS 'Episode-to-series mapping. Source: title.episode.tsv.gz';
COMMENT ON TABLE  imdb.name_basics      IS 'One row per person. Source: name.basics.tsv.gz';
COMMENT ON TABLE  imdb.title_principals IS 'Principal cast and crew. Source: title.principals.tsv.gz';
COMMENT ON TABLE  imdb.title_akas       IS 'Localised alternate titles. Source: title.akas.tsv.gz';
COMMENT ON TABLE  imdb.message_queue    IS 'Durable work queue drained via FOR UPDATE SKIP LOCKED.';

COMMENT ON COLUMN imdb.message_queue.payload      IS 'Either {"table","data"} or {"table","rows":[...]}';
COMMENT ON COLUMN imdb.message_queue.locked_until IS 'Lease expiry; NULL unless status = ''processing''';
COMMENT ON COLUMN imdb.title_basics.genres        IS 'At most 3 genres; unnest() to aggregate per genre';
