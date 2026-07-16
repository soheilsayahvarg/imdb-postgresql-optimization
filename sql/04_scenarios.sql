-- =============================================================================
--  The 8 report query scenarios (canonical source).
--
--  Run for results:
--      docker exec -i imdb_postgres psql -U imdb -d imdb -f - < sql/04_scenarios.sql
--
--  This file is ALSO the single source of truth for the EXPLAIN runs. Each query
--  sits between a "-- @scenario NN | title" marker and a "-- @end" marker so that
--  scripts/gen_explain.py can slice it out, wrap it in EXPLAIN (ANALYZE, BUFFERS),
--  and write sql/explain/scenario_NN.sql -- the query is never copied by hand.
--
--  IMPORTANT: run ANALYZE first (sql/00_analyze.sql). Straight after a bulk load
--  the planner has stale/absent statistics (reltuples = -1), which produces
--  pathological, non-reproducible baseline plans. ANALYZE gives every "before"
--  measurement a real cost model to reason against.
--
--  Column facts these queries rely on (from resources/init.sql):
--    title_basics(tconst PK, title_type, primary_title, start_year SMALLINT,
--                 runtime_minutes INT, genres TEXT[])
--    title_ratings(tconst PK, average_rating NUMERIC(3,1), num_votes INT)
--    title_crew(tconst PK, directors TEXT[], writers TEXT[])
--    title_episode(tconst PK, parent_tconst, season_number INT)
--    name_basics(nconst PK, primary_name)
--    title_principals(tconst, ordering, nconst, category, PK(tconst, ordering))
--
--  Every array is unnested with CROSS JOIN LATERAL unnest(...). unnest of a NULL
--  or empty array yields zero rows, so titles with no genres drop out naturally.
-- =============================================================================

\set ON_ERROR_STOP on
SET search_path TO imdb, public;


-- @scenario 01 | Average rating of movies by genre
-- This is the brief's own EXPLAIN example. Unnest the genres array, join ratings
-- on the title's primary key, average per genre. Inner join to ratings means only
-- rated movies contribute, which is what an "average rating" is supposed to mean.
SELECT g.genre,
       ROUND(AVG(tr.average_rating), 2) AS avg_rating,
       COUNT(*)                         AS rated_titles
FROM   imdb.title_basics tb
CROSS  JOIN LATERAL unnest(tb.genres) AS g(genre)
JOIN   imdb.title_ratings tr ON tr.tconst = tb.tconst
WHERE  tb.title_type = 'movie'
GROUP  BY g.genre
-- Order by the true average, matching the brief's example; the displayed column
-- is rounded only for readability.
ORDER  BY AVG(tr.average_rating) DESC;
-- @end


-- @scenario 02 | Top 10 movies by number of votes (min 1000 votes)
-- The vote floor and the ORDER BY ... LIMIT are both unindexed at baseline, so
-- this is a seq scan of ratings feeding a top-N sort.
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
-- @end


-- @scenario 03 | Top 20 directors by number of movies, with their average rating
-- directors is an array of nconst ids in title_crew. Unnest it, keep only movies,
-- resolve the director's name, and LEFT JOIN ratings so an unrated film still
-- counts toward the film tally while AVG ignores the NULLs.
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
-- @end


-- @scenario 04 | Yearly average rating of movies, 2000 onward
-- A grouped aggregate over a start_year range. No index on start_year at baseline,
-- so the year filter is evaluated during a seq scan.
SELECT tb.start_year,
       ROUND(AVG(tr.average_rating), 2) AS avg_rating,
       COUNT(*)                         AS rated_movies
FROM   imdb.title_basics tb
JOIN   imdb.title_ratings tr ON tr.tconst = tb.tconst
WHERE  tb.title_type = 'movie'
  AND  tb.start_year >= 2000
GROUP  BY tb.start_year
ORDER  BY tb.start_year;
-- @end


-- @scenario 05 | Actors who appeared in more than 5 distinct movie genres
-- The heaviest query in the set: it drives title_principals (~92M rows). Join to
-- the movie's genres, unnest, count distinct genres per person, keep > 5.
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
-- @end


-- @scenario 06 | Series with the most seasons (from title.episode)
-- Group episodes by their parent series and count distinct season numbers.
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
-- @end


-- @scenario 07 | Most common pair of genres in movies
-- Self-join the unnested genres of each movie on g1.genre < g2.genre, which yields
-- each unordered pair once (and never a genre with itself), then count.
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
-- @end


-- @scenario 08 | Top 5 longest movies per genre
-- A window function ranks movies within each genre by runtime. The PARTITION BY /
-- ORDER BY forces a large sort at baseline.
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
-- @end
