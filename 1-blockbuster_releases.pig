/* 
      Scheme of the movies.csv - basic information about films:
      movies.csv - basic information about films:
      id - movie identifier (primary key);
      name - the name of the film;
      date - year of release of the film;
      tagline - the slogan of the film;
      description - description of the film;
      minute - movie duration (in minutes);
      rating - average rating of the film. 
*/

/* Load Data for DataSet Movies.csv of LetterBoxd */
movies_letterboxd = LOAD '/home/hadoop/letterboxd/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (id:int, name:chararray, date:int, tagline:chararray, description:chararray, minute:int, rating:float);

/* Filtramos por si hay data corrupta que no posea id */
filtered_movies_letterboxd = FILTER movies_letterboxd BY id IS NOT NULL;

/* Filtramos las peliculas que disponen de valor de rating y de name */
valid_movies_letterboxd = FILTER filtered_movies_letterboxd BY name IS NOT NULL AND rating IS NOT NULL;

/* Filtramos las peliculas que pasan el umbral de los 4.5 puntos */
top_movies_letterboxd = FILTER valid_movies_letterboxd BY rating > 4.0;

/* Agrupamos las peliculas por anio {()()} */
grouped_by_year_letterboxd = GROUP top_movies_letterboxd BY date;

/* Contamos las peliculas de ese anio y generamos un grupo con el anio y el total de peliculas del mismo */
top_movies_count_letterboxd = FOREACH grouped_by_year_letterboxd GENERATE group AS year, COUNT(top_movies_letterboxd) AS total_movies_letterboxd;

/* DUMP top_movies_count; */  

STORE top_movies_count_letterboxd INTO '/home/hadoop/output/top_movies_count_letterboxd' USING PigStorage(',');

/*    Scheme of the title.basics.tsv file:

      (string) - alphanumeric unique identifier of the title.
      titleType (string) – the type/format of the title (e.g. movie, short,
      tvseries, tvepisode, video, etc).
      primaryTitle (string) – the more popular title / the title used by
      the filmmakers on promotional materials at the point of release.
      originalTitle (string) - original title, in the original language.
      isAdult (boolean) - 0: non-adult title; 1: adult title.
      startYear (YYYY) – represents the release year of a title. In the
      case of TV Series, it is the series start year.
      endYear (YYYY) – TV Series end year. for all other title types.
      runtimeMinutes – primary runtime of the title, in minutes.
      genres (string array) – includes up to three genres associated with
      the title. 
*/

/* Cargamos primero movies de imdb dado que este no contiene los ratings, pero a pesar de esto lo necesitamos para la info de los originalTittle y el startYear */
movies_imdb = LOAD '/home/hadoop/imdb/title.basics.tsv' USING PigStorage('\t') AS (tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:boolean, startYear:chararray, endYear:chararray, runtimeMinutes:chararray, genres:chararray);

/* Filramos por si hay data corrupota que no tenga tconst (id) */
filtered_movies_imdb = FILTER movies_imdb BY tconst IS NOT NULL;

/* Filtramos que sea tittleType movie */
filtered_movies_type_imdb = FILTER filtered_movies_imdb BY titleType == 'movie';

/* 
      Scheme of the title.ratings.tsv file:

      tconst (string) - alphanumeric unique identifier of the title.
      averageRating – weighted average of all the individual user ratings.
      numVotes - number of votes the title has received.
*/

/* Cargamos los datos de ratings.tsv (anteriormente cargamos tittle.basics.tsv porque asocia el tconst con tl originalTitle) */
ratings_imdb = LOAD '/home/hadoop/imdb/title.ratings.tsv' USING PigStorage('\t') AS (tconst:chararray, averageRating:float, numVotes:int);

/* Filtramos por para que posean raging y numVotes, y tambien tconst */ 
filtered_ratings_imdb = FILTER ratings_imdb BY tconst IS NOT NULL AND averageRating IS NOT NULL AND numVotes IS NOT NULL;

/* Filtramos con que tengan 8.0 en rating y establecemos como acotacion 1000 votos para que estos sean validos y mas veraces */
/* Establecemos en 8.0 que seria el equivalente de 4.0 en el otro dataset */
top_movies_imdb = FILTER filtered_ratings_imdb BY averageRating > 8.0 AND numVotes > 1000;

/* Ahora enlacazamos los datos para tener una relacion con el tconst, startYear, averageRating */
total_movies_imdb = JOIN filtered_movies_type_imdb BY tconst, top_movies_imdb BY tconst;

imprimir = LIMIT total_movies_imdb 10;

STORE imprimir INTO '/home/hadoop/output/imprimir' USING PigStorage(',');

/* Agrupamos por anio uno tiene startYear y otro tiene date*/
grouped_by_year = GROUP total_movies_imdb BY filtered_movies_type_imdb::startYear;

/* Contamos las peliculas de ese anio y generamos un grupo con el anio y el total de peliculas del mismo */
top_movies_count_imdb = FOREACH grouped_by_year GENERATE group AS year, COUNT(total_movies_imdb) AS total_movies;

/* DUMP top_movies_count; */

STORE top_movies_count_imdb INTO '/home/hadoop/output/top_movies_count_imdb' USING PigStorage(',');