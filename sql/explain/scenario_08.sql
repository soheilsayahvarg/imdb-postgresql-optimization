-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 08: Top 5 longest movies per genre ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
WITH ranked AS (
    SELECT g.genre,
           tb.tconst,
           tb.primary_title,
           tb.runtime_minutes,
           ROW_NUMBER() OVER (PARTITION BY g.genre
                              ORDER BY tb.runtime_minutes DESC, tb.tconst) AS rn
    FROM   imdb.title_basics tb
    CROSS  JOIN LATERAL unnest(tb.genres) AS g(genre)
    WHERE  tb.title_type = 'movie'
      AND  tb.runtime_minutes IS NOT NULL
)
SELECT genre, tconst, primary_title, runtime_minutes
FROM   ranked
WHERE  rn <= 5
ORDER  BY genre, runtime_minutes DESC, tconst;
