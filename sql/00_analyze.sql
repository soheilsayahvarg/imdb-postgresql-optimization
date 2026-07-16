-- =============================================================================
--  Refresh planner statistics before any baseline measurement.
--
--      docker exec -i imdb_postgres psql -U imdb -d imdb -f - < sql/00_analyze.sql
--
--  Immediately after a bulk load the catalog still says reltuples = -1 (never
--  analyzed) for every freshly written table, so the planner sizes joins and
--  chooses scan types blind. Running ANALYZE first is what makes the "before
--  optimisation" plans meaningful and reproducible instead of accidental.
--
--  VERBOSE prints one line per table so the run is auditable.
-- =============================================================================

\set ON_ERROR_STOP on

ANALYZE VERBOSE imdb.title_basics;
ANALYZE VERBOSE imdb.title_ratings;
ANALYZE VERBOSE imdb.title_crew;
ANALYZE VERBOSE imdb.title_episode;
ANALYZE VERBOSE imdb.name_basics;
ANALYZE VERBOSE imdb.title_principals;
ANALYZE VERBOSE imdb.title_akas;
