-- =============================================================================
--  Index tuning phase -- targeted indexes for the 8 report scenarios.
--
--  Design principle: every index below is tied to a specific WHERE/ORDER BY/JOIN
--  predicate in one or more scenarios from sql/04_scenarios.sql. No index is
--  added "just in case" -- each one is justified by name in the comment above it,
--  because every index adds write amplification (WAL, page splits, autovacuum
--  work) to every future INSERT/UPDATE on its table, and this project only pays
--  that cost once (all secondary indexes are created AFTER the bulk load in
--  Parts C/D, exactly the standard "load then index" ordering).
--
--  Run:
--      docker exec -i imdb_postgres psql -U imdb -d imdb -f - < sql/03_indexes.sql
--
--  No CREATE INDEX CONCURRENTLY: there are no concurrent writers once ingestion
--  (Part C/D) has finished, so a plain CREATE INDEX (which takes a SHARE lock --
--  blocks writers, not readers -- and builds in one pass) is both simpler and
--  cheaper here. CONCURRENTLY is the right call only when writes must continue
--  during the build.
--
--  No re-ANALYZE needed after this file: index existence doesn't change a
--  column's statistics (histogram/MCV/n_distinct), only which access paths are
--  available. That's why scripts/run_scenarios.py --phase after passes
--  --no-analyze -- the planner picks up the new indexes with the same stats
--  gathered in Part E's baseline.
-- =============================================================================

\set ON_ERROR_STOP on
SET search_path TO imdb, public;


-- ---------------------------------------------------------------------------
--  1. Composite GIN index: title_type (scalar, via btree_gin) + genres (array).
--
--  Serves scenarios 1, 3, 5, 7 -- all filter on title_type = 'movie' and none
--  of them filter genres by value (they unnest ALL genres, so a GIN index
--  cannot speed up the unnest itself -- it can only speed up *locating the
--  movie rows*). A GIN multicolumn index can be used by a query that touches
--  only ONE of its columns (unlike a B-tree, where only a leading-column
--  prefix is usable), so this single index serves the title_type-only filter
--  here AND doubles as a genre-containment index (genres @> ARRAY[...]) for
--  queries outside this report -- e.g. "list every Comedy movie" -- which the
--  schema's own btree_gin extension (resources/init.sql) was provisioned for.
--
--  Chosen over a plain partial B-tree on title_type alone: that would give up
--  free genre-containment support for a marginal equality-lookup speed gain --
--  not worth a second, overlapping index.
--
--  GIN is a bitmap-only access method (no amgettuple), so PostgreSQL can only
--  reach it via a Bitmap Index Scan + Bitmap Heap Scan, never a plain
--  Index Scan -- that's the expected "before -> after" node transition for
--  scenarios 1/3/5/7 (Seq Scan -> Bitmap Heap Scan / Bitmap Index Scan).
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_basics_type_genres_gin
    ON imdb.title_basics
    USING GIN (title_type, genres);


-- ---------------------------------------------------------------------------
--  2. Partial B-tree on start_year, scoped to movies.
--
--  Serves scenario 4 (yearly average rating, start_year >= 2000 AND
--  title_type = 'movie'). GIN has no notion of ordering, so it cannot serve a
--  range predicate like ">= 2000" -- a B-tree is the correct structure. The
--  partial WHERE clause folds the title_type filter into the index itself, so
--  an Index Scan on this index directly yields "movies from year >= 2000".
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_basics_movie_year
    ON imdb.title_basics (start_year)
    WHERE title_type = 'movie';


-- ---------------------------------------------------------------------------
--  3. Partial B-tree on runtime_minutes, scoped to movies with a known runtime.
--
--  Serves scenario 8 (top 5 longest movies per genre; requires
--  runtime_minutes IS NOT NULL AND title_type = 'movie'). Same reasoning as
--  above: the partial predicate matches the scenario's WHERE clause exactly.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_basics_movie_runtime
    ON imdb.title_basics (runtime_minutes)
    WHERE title_type = 'movie' AND runtime_minutes IS NOT NULL;


-- ---------------------------------------------------------------------------
--  4. Partial B-tree on num_votes, descending, restricted to the >= 1000
--     threshold the scenario itself uses.
--
--  Serves scenario 2 (top 10 movies by num_votes >= 1000, ORDER BY num_votes
--  DESC LIMIT 10). This is the textbook "index for Top-N": the plan becomes
--  an Index Scan reading num_votes from the highest value down, probing
--  title_basics (indexed on tconst, its PK) per row for the title_type check,
--  stopping once LIMIT 10 is satisfied -- instead of sorting every rated
--  title. The partial predicate matches the query's own floor and shrinks the
--  index to exclude the (large) majority of titles under 1000 votes.
--
--  DESC is written explicitly for readability; PostgreSQL can scan a plain
--  ascending B-tree backwards just as cheaply, but writing DESC also lets a
--  plain forward Index Scan satisfy "ORDER BY num_votes DESC" directly.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_ratings_votes_desc
    ON imdb.title_ratings (num_votes DESC)
    WHERE num_votes >= 1000;


-- ---------------------------------------------------------------------------
--  5. Partial composite B-tree on title_principals, scoped to acting credits.
--
--  Serves scenario 5 (actors in more than 5 distinct genres) -- the heaviest
--  query in the set, driving the ~90M-row title_principals table. category
--  has roughly a dozen distinct values (actor, actress, director, writer,
--  producer, ...); restricting the index to 'actor'/'actress' shrinks it
--  relative to the full table. Leading column is nconst (the GROUP BY key)
--  so one person's acting credits are adjacent in the index, and tconst is
--  included so the join to title_basics can use the index tuple directly.
--
--  Note: title_principals' own primary key, PK(tconst, ordering), already
--  gives the planner a tconst-leading path, so a Nested Loop driven from the
--  (much smaller) movie-filtered title_basics side was already available
--  before this index existed. This index adds the complementary
--  nconst-leading path the PK cannot provide, which is what the GROUP BY
--  nb.nconst in this scenario needs.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_principals_cast
    ON imdb.title_principals (nconst, tconst)
    WHERE category IN ('actor', 'actress');


-- ---------------------------------------------------------------------------
--  6. Partial composite B-tree on title_episode, scoped to numbered episodes.
--
--  Serves scenario 6 (series with the most seasons). parent_tconst has no
--  index today (only tconst, the PK, does), so the join to title_basics
--  forces a full scan of title_episode. season_number is folded in as the
--  second key so COUNT(DISTINCT season_number) can be computed from the index
--  without revisiting the heap for that column, and the partial WHERE clause
--  matches the scenario's own season_number IS NOT NULL filter exactly.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_title_episode_parent
    ON imdb.title_episode (parent_tconst, season_number)
    WHERE season_number IS NOT NULL;


-- ---------------------------------------------------------------------------
--  Deliberately NOT indexed:
--
--    title_crew.directors, and title_basics.genres as a LONE array index --
--    scenarios 1/3/5/7/8 all unnest() the ENTIRE array; none of them filter
--    "does this array contain value X", the only predicate shape a GIN array
--    index accelerates. unnest() must still visit every element of every
--    qualifying row's array regardless of any index -- PostgreSQL forbids
--    set-returning functions in expression indexes, so there is no functional
--    index that changes this; only a normalized junction table would (a
--    schema change, out of scope for an index-tuning pass).
--
--    Any *.metadata JSONB column -- none of the 8 scenarios read a metadata
--    column. An index that serves no query in this report is pure write
--    overhead with no offsetting read benefit.
-- ---------------------------------------------------------------------------
