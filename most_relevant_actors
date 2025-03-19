-- Modificar rutas si es necesario.

REGISTER '/home/hadoop/pig-0.16.0/lib/piggybank.jar';

-- Cargar movies.csv (Letterboxd)
movies = LOAD '/home/hadoop/imdb_data/csv/movies.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (id:chararray, name:chararray, date:int, tagline:chararray, description:chararray, minute:int, rating:float);

-- Filtrar películas con rating alto
high_rated_movies = FILTER movies BY rating > 4.0;

-- Cargar géneros CSV
genres = LOAD '/home/hadoop/imdb_data/csv/genres.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (id:chararray, genre:chararray);

-- Cargar actores CSV
actors = LOAD '/home/hadoop/imdb_data/csv/actors.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (id:chararray, actor_name:chararray, role:chararray);

-- JOIN películas con géneros
movies_genres = JOIN high_rated_movies BY id, genres BY id;

-- Cargar datasets IMDB (TSV)
title_basics = LOAD '/home/hadoop/imdb_data/tsv/title.basics.tsv'
    USING PigStorage('\t') AS (
        tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray,
        isAdult:int, startYear:int, endYear:int, runtimeMinutes:chararray, genres:chararray
    );

title_ratings = LOAD '/home/hadoop/imdb_data/tsv/title.ratings.tsv'
    USING PigStorage('\t') AS (
        tconst:chararray, averageRating:float, numVotes:int
    );

title_principals = LOAD '/home/hadoop/imdb_data/tsv/title.principals.tsv'
    USING PigStorage('\t') AS (
        tconst:chararray, ordering:int, nconst:chararray, category:chararray, job:chararray, characters:chararray
    );

name_basics = LOAD '/home/hadoop/imdb_data/tsv/name.basics.tsv'
    USING PigStorage('\t') AS (
        nconst:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray
    );

-- JOIN por nombre y año en vez de ID
movies_details = JOIN high_rated_movies BY (name,date), title_basics BY (primaryTitle,startYear);

-- JOIN películas con ratings usando tconst
movies_ratings = JOIN movies_details BY title_basics::tconst, title_ratings BY tconst;

-- JOIN con actores principales IMDB
movies_principals = JOIN movies_ratings BY title_basics::tconst, title_principals BY tconst;

-- JOIN actores principales con nombres IMDB
principals_names = JOIN movies_principals BY title_principals::nconst, name_basics BY nconst;

-- Proyectar columnas necesarias claramente
actors_detailed = FOREACH principals_names GENERATE
    movies_principals::movies_ratings::movies_details::high_rated_movies::id AS movie_id,
    movies_principals::movies_ratings::title_ratings::averageRating AS averageRating,
    movies_principals::movies_ratings::title_ratings::numVotes AS numVotes,
    name_basics::primaryName AS actor_name;

-- JOIN final con géneros usando movie_id
final_join = JOIN actors_detailed BY movie_id, movies_genres BY high_rated_movies::id;

-- Proyección final
final_projection = FOREACH final_join GENERATE
    movies_genres::genres::genre AS genre,
    actor_name,
    averageRating,
    numVotes;

-- Agrupar por (género, actor)
grouped_genre_actor = GROUP final_projection BY (genre, actor_name);

-- Calcular métricas
actor_influence = FOREACH grouped_genre_actor GENERATE
    FLATTEN(group) AS (genre, actor),
    COUNT(final_projection) AS movie_count,
    AVG(final_projection.averageRating) AS avg_rating,
    SUM(final_projection.numVotes) AS total_votes,
    (SUM(final_projection.numVotes) * AVG(final_projection.averageRating)) AS influence_score;

-- Ordenar resultados
sorted_actor_influence = ORDER actor_influence BY influence_score DESC;

-- Guardar resultados
STORE sorted_actor_influence INTO '/home/hadoop/imdb_data/output/actor_influence' USING PigStorage(',');
