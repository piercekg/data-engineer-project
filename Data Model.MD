## Diagram
See `Database ER diagram.jpeg` in this directory

## Schema
See `schema.sql` in this directory for the SQL used to create the database and tables used

## Description
In order to answer the questions posed, I created a data model with three entities: movies, production companies, and genres. The movies table tracks the data for each movie needed to answer the questions posed in this challenge, including budget, revenue, popularity, and release_date, as well as title to allow for future questions comparing individual movies. The genres and production_companies tables track genre names and production company names, respectively. Because the relationship between movies and genres is many-to-many, entries in the two tables are linked in a join or association table, movies_genres, in which each entry is a movie_id and corresponding genre_id. Similarly, the relationship between movies and production companies is many-to-many, so those two tables are again linked by a join/association table, production_companies_movies, in which each entry is a movie_id and corresponding production_company_id. My current schema does not include any indexes, but if I knew that certain queries were going to be particularly common, or in this case if users were going to be querying data by movie, I would consider indexing.

## Queries
See `db.py` in this directory for the SQL queries used to put the questions from this challenge to the data model. The data model allows for these questions and more to be answered without any processing of the results in python.