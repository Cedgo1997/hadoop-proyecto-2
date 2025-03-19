-- Registrar Piggybank
REGISTER '/home/hadoop/pig/lib/piggybank.jar';

-- Cargar movies.csv con Piggybank CSVExcelStorage (para manejar comas internas correctamente)
movies = LOAD 

'/home/hadoop/hadoop_local/movies_data/letterboxd/movies.csv'
         USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
         AS (id:int, name:chararray, date:int, tagline:chararray, description:chararray, minute:int, rating:float);

-- Seleccionar solo columnas esenciales
movies_selected = FOREACH movies GENERATE id, minute, rating;

-- Filtrar por duración (>=60 min)
filtered_movies = FILTER movies_selected BY minute >= 60;

-- Cargar dataset de géneros
genres = LOAD '/home/hadoop/hadoop_local/movies_data/letterboxd/genres.csv'
         USING PigStorage(',')
         AS (id:int, genre:chararray);

-- JOIN películas con géneros usando id
movies_with_genres = JOIN filtered_movies BY id, genres BY id;

-- Agrupar por género
grouped_by_genre = GROUP movies_with_genres BY genre;

-- Calcular promedio de rating por género
avg_rating_per_genre = FOREACH grouped_by_genre GENERATE
                       group AS genre,
                       AVG(movies_with_genres.rating) AS avg_rating;

-- Ordenar alfabéticamente
ordered_avg_genre = ORDER avg_rating_per_genre BY genre ASC;

-- Almacenar resultados
STORE ordered_avg_genre INTO '/home/hadoop/hadoop_local/output/letterboxd_avg_rating'
    USING PigStorage(',');