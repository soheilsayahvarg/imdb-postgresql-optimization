-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 04: Yearly average rating of movies, 2000 onward ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT tb.start_year,
       ROUND(AVG(tr.average_rating), 2) AS avg_rating,
       COUNT(*)                         AS rated_movies
FROM   imdb.title_basics tb
JOIN   imdb.title_ratings tr ON tr.tconst = tb.tconst
WHERE  tb.title_type = 'movie'
  AND  tb.start_year >= 2000
GROUP  BY tb.start_year
ORDER  BY tb.start_year;
