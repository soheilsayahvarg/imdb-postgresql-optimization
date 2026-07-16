-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 05: Actors who appeared in more than 5 distinct movie genres ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT nb.nconst,
       nb.primary_name,
       COUNT(DISTINCT g.genre) AS genre_count
FROM   imdb.title_principals tp
JOIN   imdb.title_basics tb ON tb.tconst = tp.tconst AND tb.title_type = 'movie'
CROSS  JOIN LATERAL unnest(tb.genres) AS g(genre)
JOIN   imdb.name_basics nb ON nb.nconst = tp.nconst
WHERE  tp.category IN ('actor', 'actress')
GROUP  BY nb.nconst, nb.primary_name
HAVING COUNT(DISTINCT g.genre) > 5
ORDER  BY genre_count DESC, nb.primary_name;
