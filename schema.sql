DROP DATABASE IF EXISTS movies;
CREATE DATABASE movies;
\c movies;

/*budget,genres,id,imdb_id,original_title,popularity,production_companies,release_date,revenue,title,vote_average,vote_count*/
DROP TABLE IF EXISTS production_companies;
CREATE TABLE production_companies(
  id integer PRIMARY KEY,
  name varchar(70)
);

DROP TABLE IF EXISTS genres;
CREATE TABLE genres(
  id integer PRIMARY KEY,
  name varchar(40)
);

DROP TABLE IF EXISTS movies;
CREATE TABLE movies(
  id integer PRIMARY KEY,
  budget integer,
  revenue integer,
  popularity real,
  release_date date
);

DROP TABLE IF EXISTS production_companies_movies;
CREATE TABLE production_companies_movies(
  production_company_id integer,
  movie_id integer,
  FOREIGN KEY (production_company_id) REFERENCES production_companies(id),
  FOREIGN KEY (movie_id) REFERENCES movies(id)
);

DROP TABLE IF EXISTS movies_genres;
CREATE TABLE movies_genres(
  movie_id integer,
  genre_id integer,
  FOREIGN KEY (movie_id) REFERENCES movies(id),
  FOREIGN KEY (genre_id) REFERENCES genres(id)
);