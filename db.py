import psycopg2
import asyncio

# Connect to your postgres DB
conn = psycopg2.connect("dbname=movies user=kevin")
"""conn = psycopg2.connect(
  host = "localhost",
  database = "test",
  user = "kevin",
  password = "password"
)"""

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
#cur.execute("SELECT * FROM movies")

# Retrieve query results
#records = cur.fetchall()
#print(records)
def insert_genre(genre):
  SQL = "INSERT INTO genres (id, name) VALUES (%s, %s);"
  data = [genre['id'], genre['name']]
  result = cur.execute(SQL, data)
  return result

def insert_movie(movie):
  SQL = "INSERT INTO movies (id, budget, revenue, popularity, release_date) VALUES (%s, %s, %s, %s, %s);"
  data = [movie['id'], movie['budget'], movie['revenue'], movie['popularity'], movie['release_date']]
  result = cur.execute(SQL, data)
  return result

def insert_production_company(production_company):
  SQL = "INSERT INTO production_companies (id, name) VALUES (%s, %s);"
  data = [production_company['id'], production_company['name']]
  result = cur.execute(SQL, data)
  return result

def insert_movie_genre(movie, genre):
  SQL = "INSERT INTO movies_genres (movie_id, genre_id) VALUES (%s, %s);"
  data = [movie['id'], genre['id']]
  result = cur.execute(SQL, data)
  return result

def insert_production_company_movie(production_company, movie):
  SQL = "INSERT INTO production_companies_movies (production_company_id, movie_id) VALUES (%s, %s);"
  data = [production_company['id'], movie['id']]
  result = cur.execute(SQL, data)
  return result