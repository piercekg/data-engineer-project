DROP DATABASE IF EXISTS movies;
CREATE DATABASE movies;
\c movies;

DROP TABLE IF EXISTS production_companies;
CREATE TABLE production_companies(
  id integer PRIMARY KEY,
  name varchar(150)
);

DROP TABLE IF EXISTS genres;
CREATE TABLE genres(
  id integer PRIMARY KEY,
  name varchar(40)
);

DROP TABLE IF EXISTS movies;
CREATE TABLE movies(
  id integer PRIMARY KEY,
  title varchar(150),
  budget integer,
  revenue bigint,
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
