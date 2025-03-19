-- Se cargan los datos de movies.csv usando el módulo CSVExcelStorage()
movies_letterboxd = LOAD '/home/hadoop/datasets/letterboxd/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (id:int, name:chararray, date:int, tagline:chararray, description:chararray, minute:int, rating:float);

-- Filtramos la data que no tenga valores nulos
filtered_movies_letterboxd = FILTER movies_letterboxd BY id IS NOT NULL;

valid_movies_letterboxd = FILTER filtered_movies_letterboxd BY name IS NOT NULL AND rating IS NOT NULL;

-- Filtramos las películas que pasan el umbral de los 4.5 puntos
top_movies_letterboxd = FILTER valid_movies_letterboxd BY rating > 4.0;

-- Agrupamos las películas por año
grouped_by_year_letterboxd = GROUP top_movies_letterboxd BY date;

-- Contamos las películas de ese año y generamos un grupo con el año y el total de películas del mismo. Lo uniremos al de IMDb mas adelante
top_movies_count_letterboxd = FOREACH grouped_by_year_letterboxd GENERATE group AS year, COUNT(top_movies_letterboxd) AS total_movies_letterboxd; 



-- Cargamos primero movies de imdb dado que este no contiene los ratings, pero a pesar de esto lo necesitamos para la info de los originalTitle y el startYear
movies_imdb = LOAD '/home/hadoop/datasets/imdb/title.basics.tsv' USING PigStorage('\t') AS (tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:boolean, startYear:chararray, endYear:chararray, runtimeMinutes:chararray, genres:chararray);

-- Filtramos por si hay data corrupta que no tenga tconst (id) 
filtered_movies_imdb = FILTER movies_imdb BY (tconst != '\\N');

-- Filtramos que sea película
filtered_movies_type_imdb = FILTER filtered_movies_imdb BY titleType == 'movie';

-- Cargamos los datos de ratings.tsv (anteriormente cargamos title.basics.tsv porque asocia el tconst con l el nombre y año)
ratings_imdb = LOAD '/home/hadoop/datasets/imdb/title.ratings.tsv' USING PigStorage('\t') AS (tconst:chararray, averageRating:float, numVotes:int);

-- Filtramos por para que posean tconst
filtered_ratings_imdb = FILTER ratings_imdb BY (tconst != '\\N');

-- Filtramos con que tengan 8.0 en rating y establecemos como acotación 2100 votos
-- Establecemos en 8.0 que sería el equivalente de 4.0 en el otro dataset 
top_movies_imdb = FILTER filtered_ratings_imdb BY averageRating > 8.0 AND numVotes > 2100;

-- Ahora enlazamos los datos para tener una relacion con el tconst, startYear, averageRating
total_movies_imdb = JOIN filtered_movies_type_imdb BY tconst, top_movies_imdb BY tconst;


-- Agrupamos por año uno tiene startYear y otro tiene date
grouped_by_year = GROUP total_movies_imdb BY filtered_movies_type_imdb::startYear;

-- Contamos las películas de ese año y generamos un grupo con el año y el total de películas del mismo
top_movies_count_imdb = FOREACH grouped_by_year GENERATE group AS year, COUNT(total_movies_imdb) AS total_movies;

-- Hacemos join de los datos en ambos datasets para tenerlos en un archivo
top_movies_imdb_letterboxd = JOIN top_movies_count_imdb BY (int) year, top_movies_count_letterboxd by (int) year;

STORE top_movies_imdb_letterboxd INTO '/home/hadoop/output/top_movies_count_imdb_letterboxd' USING PigStorage(',');

