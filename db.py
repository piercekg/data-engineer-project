import psycopg2
from pygrok import Grok
import asyncio

# Connect to your postgres DB
#conn = psycopg2.connect("dbname=movies user=kevin")
conn = psycopg2.connect(
  host = "localhost",
  database = "movies",
  user = "user1",
  password = "password"
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
#cur.execute("SELECT * FROM movies")

# Retrieve query results
#records = cur.fetchall()
#print(records)
async def query(sql, data):
  #print(sql, data)
  cur.execute(sql, data)
  conn.commit()

async def insert_genre(genre):
  SQL = "INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;"
  data = [genre['id'], genre['name']]
  result = await query(SQL, data)
  return result

pattern = '%{NUMBER:budget:int}'
grok = Grok(pattern)

async def insert_movie(movie):
  SQL = "INSERT INTO movies (id, budget, revenue, popularity, release_date) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;"
  if not movie['release_date']:
    movie['release_date'] = None
  budget = grok.match(str(movie['budget']))
  if not budget:
    movie['budget'] = 0
  revenue = grok.match(str(movie['revenue']))
  if not revenue:
    movie['revenue'] = 0
  if not movie['popularity']:
    movie['popularity'] = 0.0
  data = [int(movie['id']), int(movie['budget']), int(movie['revenue']), float(movie['popularity']), movie['release_date']]
  result = await query(SQL, data)
  return result

#cur.execute("INSERT INTO movies (id, budget, revenue, popularity, release_date) VALUES (%s, %s, %s, %s, %s);", [862, 30000000, 373554033, 21.946943, '1995-10-30'])
#conn.commit()
#cur.execute("SELECT * FROM movies")
#result = cur.fetchone()
#print(result)

async def insert_production_company(production_company):
  SQL = "INSERT INTO production_companies (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;"
  data = [production_company['id'], production_company['name']]
  result = await query(SQL, data)
  return result

async def insert_movie_genre(movie, genre):
  SQL = "INSERT INTO movies_genres (movie_id, genre_id) VALUES (%s, %s);"
  data = [movie['id'], genre['id']]
  result = await query(SQL, data)
  return result

async def insert_production_company_movie(production_company, movie):
  SQL = "INSERT INTO production_companies_movies (production_company_id, movie_id) VALUES (%s, %s);"
  data = [production_company['id'], movie['id']]
  result = await query(SQL, data)
  return result

async def get_budget_by_company(company_id):
  SQL = "SELECT SUM(m.budget) FROM movies m, production_companies_movies p where m.id = p.movie_id and p.production_company_id = 559;"

async def get_revenue_by_genre():
  SQL = "SELECT SUM(m.revenue) FROM movies m, movies_genres j, genres g WHERE m.id = j.movie_id AND j.genre_id = g.id ORDER BY SUM(m.revenue) DESC;"