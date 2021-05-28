import psycopg2
from datetime import datetime
import asyncio
from config import config

# Connect to your postgres DB
#conn = psycopg2.connect("dbname=movies user=kevin")
conn = psycopg2.connect(
  host = config['host'],
  database = config['database'],
  user = config['user'],
  password = config['password']
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
#cur.execute("SELECT * FROM movies")

# Retrieve query results
#records = cur.fetchall()
#print(records)
# INSERT function
async def insert_query(sql, data):
  cur.execute(sql, data)
  conn.commit()

# INSERT queries
async def insert_movie(movie):
  SQL = "INSERT INTO movies (id, title, budget, revenue, popularity, release_date) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title RETURNING *;"
  data = [int(movie['id']), movie['title'], int(movie['budget']), int(movie['revenue']), float(movie['popularity']), movie['release_date']]
  result = await insert_query(SQL, data)
  if result:
    print(str(result))
    with open('insert_err_log.txt', 'a') as errlog:
      errlog.write(str(datetime.now()) + '-' + str(result) + '\n')
  return result

async def insert_genre(genre):
  SQL = "INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;"
  data = [int(genre['id']), genre['name']]
  result = await insert_query(SQL, data)
  return result

async def insert_production_company(production_company):
  SQL = "INSERT INTO production_companies (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;"
  data = [int(production_company['id']), production_company['name']]
  result = await insert_query(SQL, data)
  return result

async def insert_movie_genre(movie, genre):
  SQL = "INSERT INTO movies_genres (movie_id, genre_id) VALUES (%s, %s);"
  data = [int(movie['id']), int(genre['id'])]
  result = await insert_query(SQL, data)
  return result

async def insert_production_company_movie(production_company, movie):
  SQL = "INSERT INTO production_companies_movies (production_company_id, movie_id) VALUES (%s, %s);"
  data = [int(production_company['id']), int(movie['id'])]
  result = await insert_query(SQL, data)
  return result

# SELECT function
async def select_query(sql, path, header):
  with open('./output/' + path, 'w') as output:
    output.write(header + '\n')
    cur.execute(sql)
    if len(header.split(',')) == 4:
      for record in cur:
        output.write(str(record[0]) + ',' + str(record[1]) + ',' + str(int(record[2])) + ',' + str(int(record[3])) + '\n')
    else:
      for record in cur:
        output.write(str(record[0]) + ',' + str(int(record[1])) + ',' + str(float(record[2])) + '\n')

# Movie Genre Details
async def select_budget_by_genre_by_year():
  SQL = "SELECT g.name, DATE_PART('year', m.release_date) AS year, SUM(m.budget) AS total_budget FROM movies m, movies_genres j, genres g WHERE m.id = j.movie_id AND j.genre_id = g.id GROUP BY g.name, year ORDER BY year DESC, total_budget DESC;"
  path = 'budget_by_genre_by_year.csv'
  header = 'genre,year,total_budget'
  result = await select_query(SQL, path, header)
  return result

async def select_revenue_by_genre_by_year():
  SQL = "SELECT g.name, DATE_PART('year', m.release_date) AS year, SUM(m.revenue) AS total_revenue FROM movies m, movies_genres j, genres g WHERE m.id = j.movie_id AND j.genre_id = g.id GROUP BY g.name, year ORDER BY year DESC, total_revenue DESC;"
  path = 'revenue_by_genre_by_year.csv'
  header = 'genre,year,total_revenue'
  result = await select_query(SQL, path, header)
  return result

async def select_profit_by_genre_by_year():
  SQL = "SELECT g.name, DATE_PART('year', m.release_date) AS year, SUM(m.revenue) - SUM(m.budget) AS profit FROM movies m, movies_genres j, genres g WHERE m.id = j.movie_id AND j.genre_id = g.id GROUP BY g.name, year ORDER BY year DESC, profit DESC;"
  path = 'profit_by_genre_by_year.csv'
  header = 'genre,year,profit'
  result = await select_query(SQL, path, header)
  return result

async def select_popularity_by_genre_by_year():
  SQL = "SELECT g.name, DATE_PART('year', m.release_date) AS year, AVG(m.popularity) AS average_popularity FROM movies m, movies_genres j, genres g WHERE m.id = j.movie_id AND j.genre_id = g.id GROUP BY g.name, year ORDER BY year DESC, average_popularity DESC;"
  path = 'popularity_by_genre_by_year.csv'
  header = 'genre,year,average_popularity'
  result = await select_query(SQL, path, header)
  return result

# Production Company Details:
async def select_budget_by_company_by_year():
  SQL = "SELECT p.name, DATE_PART('year', m.release_date) AS year, SUM(m.budget) AS total_budget FROM movies m, production_companies_movies j, production_companies p WHERE m.id = j.movie_id AND j.production_company_id = p.id GROUP BY p.name, year ORDER BY year DESC, total_budget DESC;"
  path = 'budget_by_company_by_year.csv'
  header = 'production_company,year,total_budget'
  result = await select_query(SQL, path, header)
  return result

async def select_revenue_by_company_by_year():
  SQL = "SELECT p.name, DATE_PART('year', m.release_date) AS year, SUM(m.revenue) AS total_revenue FROM movies m, production_companies_movies j, production_companies p WHERE m.id = j.movie_id AND j.production_company_id = p.id GROUP BY p.name, year ORDER BY year DESC, total_revenue DESC;"
  path = 'revenue_by_company_by_year.csv'
  header = 'production_company,year,total_revenue'
  result = await select_query(SQL, path, header)
  return result

async def select_profit_by_company_by_year():
  SQL = "SELECT p.name, DATE_PART('year', m.release_date) AS year, SUM(m.revenue) - SUM(m.budget) AS profit FROM movies m, production_companies_movies j, production_companies p WHERE m.id = j.movie_id AND j.production_company_id = p.id GROUP BY p.name, year ORDER BY year DESC, profit DESC;"
  path = 'profit_by_company_by_year.csv'
  header = 'production_company,year,profit'
  result = await select_query(SQL, path, header)
  return result

async def select_popularity_by_company_by_year():
  SQL = "SELECT p.name, DATE_PART('year', m.release_date) AS year, AVG(m.popularity) AS average_popularity FROM movies m, production_companies_movies j, production_companies p WHERE m.id = j.movie_id AND j.production_company_id = p.id GROUP BY p.name, year ORDER BY year DESC, average_popularity DESC;"
  path = 'popularity_by_company_by_year.csv'
  header = 'production_company,year,average_popularity'
  result = await select_query(SQL, path, header)
  return result

async def select_releases_by_company_by_genre_by_year():
  SQL = "SELECT company, genre, DATE_PART('year', m.release_date) AS year, COUNT(z.movie_id) AS releases FROM movies m JOIN (SELECT company, genre, x.movie_id FROM (SELECT p.name AS company, j.movie_id FROM production_companies p, production_companies_movies j WHERE j.production_company_id = p.id) x JOIN (SELECT g.name AS genre, k.movie_id FROM genres g, movies_genres k WHERE k.genre_id = g.id) y ON x.movie_id = y.movie_id) z ON z.movie_id = m.id GROUP BY company, genre, year ORDER BY company, genre, year DESC;"
  path = 'releases_by_company_by_genre_by_year.csv'
  header = 'production_company,genre,year,releases'
  result = await select_query(SQL, path, header)
  return result

"""
async def get_budget_by_company(company_id):
  SQL = "SELECT SUM(m.budget) FROM movies m, production_companies_movies p where m.id = p.movie_id and p.production_company_id = 559;"
"""