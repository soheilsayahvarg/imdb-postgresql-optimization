-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 07: Most common pair of genres in movies ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT g1.genre AS genre_a,
       g2.genre AS genre_b,
       COUNT(*) AS pair_count
FROM   imdb.title_basics tb
CROSS  JOIN LATERAL unnest(tb.genres) AS g1(genre)
CROSS  JOIN LATERAL unnest(tb.genres) AS g2(genre)
WHERE  tb.title_type = 'movie'
  AND  g1.genre < g2.genre
GROUP  BY g1.genre, g2.genre
ORDER  BY pair_count DESC
LIMIT  20;
