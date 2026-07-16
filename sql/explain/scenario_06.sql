-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 06: Series with the most seasons (from title.episode) ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT pb.tconst,
       pb.primary_title,
       COUNT(DISTINCT te.season_number) AS seasons,
       COUNT(*)                         AS episodes
FROM   imdb.title_episode te
JOIN   imdb.title_basics pb ON pb.tconst = te.parent_tconst
WHERE  te.season_number IS NOT NULL
GROUP  BY pb.tconst, pb.primary_title
ORDER  BY seasons DESC, episodes DESC
LIMIT  20;
