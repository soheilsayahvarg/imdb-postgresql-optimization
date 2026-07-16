-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 01: Average rating of movies by genre ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT g.genre,
       ROUND(AVG(tr.average_rating), 2) AS avg_rating,
       COUNT(*)                         AS rated_titles
FROM   imdb.title_basics tb
CROSS  JOIN LATERAL unnest(tb.genres) AS g(genre)
JOIN   imdb.title_ratings tr ON tr.tconst = tb.tconst
WHERE  tb.title_type = 'movie'
GROUP  BY g.genre
ORDER  BY AVG(tr.average_rating) DESC;
