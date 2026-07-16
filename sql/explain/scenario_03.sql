-- Auto-generated from sql/04_scenarios.sql by scripts/gen_explain.py.
-- Do not edit; edit the source scenario and regenerate.
\set ON_ERROR_STOP on
SET search_path TO imdb, public;
\qecho '===== SCENARIO 03: Top 20 directors by number of movies, with their average rating ====='
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT nb.nconst,
       nb.primary_name,
       COUNT(*)                         AS num_movies,
       ROUND(AVG(tr.average_rating), 2) AS avg_rating
FROM   imdb.title_crew tc
CROSS  JOIN LATERAL unnest(tc.directors) AS d(nconst)
JOIN   imdb.title_basics tb  ON tb.tconst = tc.tconst AND tb.title_type = 'movie'
JOIN   imdb.name_basics  nb  ON nb.nconst = d.nconst
LEFT   JOIN imdb.title_ratings tr ON tr.tconst = tc.tconst
GROUP  BY nb.nconst, nb.primary_name
ORDER  BY num_movies DESC, AVG(tr.average_rating) DESC NULLS LAST
LIMIT  20;
