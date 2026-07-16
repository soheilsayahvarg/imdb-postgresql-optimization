-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 02: Top 10 movies by number of votes (min 1000 votes) ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT tb.tconst,
       tb.primary_title,
       tb.start_year,
       tr.average_rating,
       tr.num_votes
FROM   imdb.title_ratings tr
JOIN   imdb.title_basics tb ON tb.tconst = tr.tconst
WHERE  tb.title_type = 'movie'
  AND  tr.num_votes >= 1000
ORDER  BY tr.num_votes DESC
LIMIT  10;
