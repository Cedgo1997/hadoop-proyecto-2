-- Load basics.tsv (id, runtimeMinutes, genres)
basics = LOAD '/home/hadoop/hadoop_local/movies_data/imdb/title.basics.tsv' USING PigStorage('\t') AS (
  tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray,
  isAdult:int, startYear:int, endYear:int, runtimeMinutes:chararray, genres:chararray
);

-- Filtra instancias no nulas y duracion >= 60 minutos
basics_valid = FILTER basics BY (runtimeMinutes != '\\N') AND ((int)runtimeMinutes >= 60);

-- Cargar ratings.tsv (id, averageRating)
ratings = LOAD '/home/hadoop/hadoop_local/movies_data/imdb/title.ratings.tsv' USING PigStorage('\t') AS (
  tconst:chararray, averageRating:float, numVotes:int
);

-- JOIN por tconst
joined_data = JOIN basics_valid BY tconst, ratings BY tconst;

-- joined_data contiene (basics_valid::tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres, ratings::tconst, averageRating, numVotes)

 -- Dividir múltiples géneros
genres_expanded = FOREACH joined_data GENERATE
  averageRating,
  FLATTEN(STRSPLIT(genres, ',')) AS genre:chararray;

-- Agrupar por género
grouped_by_genre = GROUP genres_expanded BY genre;

-- Promedio rating por género
avg_genre_rating = FOREACH grouped_by_genre GENERATE
  group AS genre,
  AVG(genres_expanded.averageRating) AS avg_rating;

-- Filtrar los datos agrupados para no tomar en cuenta los cortos y los Nulos
avg_genre_rating_valid = FILTER avg_genre_rating BY (genre != '\\N') AND (genre != 'Short'); 

-- Ordenar alfabéticamente
ordered_avg_genre = ORDER avg_genre_rating_valid BY genre ASC;

-- Guarda resultado para gráfico
STORE ordered_avg_genre INTO '/home/hadoop/hadoop_local/output/imdb_avg_rating' USING PigStorage(',');