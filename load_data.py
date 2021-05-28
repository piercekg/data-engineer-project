import csv
import ast
import asyncio
import db
from datetime import datetime
from pygrok import Grok
import re

def pattern_match(value, pattern):
  grok = Grok(pattern)
  result = grok.match(value)
  return result

async def load_data(path):
  errlog = open("err_log.txt", "w")
  errlog.write('Start: ' + str(datetime.now()) + '\n')

  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:

      if row['id']:
        movie_id = pattern_match(str(row['id']), '%{INT:id}')
      if row['title']:
        title = re.search(r"^[0-9a-zA-Z](?:[ '.\-0-9a-zA-Z]*[0-9a-zA-Z])?$", row['title'])
      if row['budget']:
        budget = pattern_match(str(row['budget']), '%{INT:budget}')
      if row['revenue']:
        revenue = pattern_match(str(row['revenue']), '%{INT:revenue}')
      if row['popularity']:
        popularity = pattern_match(str(row['popularity']), '%{NUMBER:popularity}')
      if row['release_date']:
        release = pattern_match(str(row['release_date']), '%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}')
      if release:
        release_date = '%s-%s-%s' % (release['year'], release['month'], release['day'])

      if movie_id is None or title is None or budget is None or revenue is None or popularity is None or release_date is None:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')
      else:
        movie = {"id": movie_id['id'], "title": title.group(), "budget": budget['budget'], "revenue": revenue['revenue'], "popularity": popularity['popularity'], "release_date": release_date}
        x = await db.insert_movie(movie)

      movie_genres = ['Animation', 'Comedy', 'Family' ,'Adventure' ,'Fantasy' ,'Romance' ,'Drama', 'Action', 'Crime', 'Thriller', 'Horror', 'History', 'Science', 'Fiction Mystery', 'War', 'Foreign', 'Music', 'Documentary', 'Western', 'TV Movie']
      genres = ast.literal_eval(row['genres'])
      if genres:
        for genre in genres:
          if genre['name'] in movie_genres:
            y = await db.insert_genre(genre)
            z = await db.insert_movie_genre(movie, genre)
          else:
            errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')
      elif genres is None:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

      production_companies = ast.literal_eval(str(row['production_companies']))
      if production_companies:
        for production_company in production_companies:
          a = await db.insert_production_company(production_company)
          b = await db.insert_production_company_movie(production_company, movie)
      elif production_companies is None:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

  errlog.write('End: ' + str(datetime.now()))
  errlog.close()
asyncio.run(load_data('./the-movies-dataset/movies_metadata.csv'))

"""
def load_data(path):

  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:

      if row['title']:
        title = re.search(r"^[0-9a-zA-Z](?:[ '.\-0-9a-zA-Z]*[0-9a-zA-Z])?$", row['title'])
        print(title.group())
      else:
        title = None

load_data('./the-movies-dataset/movies_metadata.csv')
"""
"""
async def load_data(path):
  with open(path, newline='') as movie_data:
    reader = csv.reader(movie_data)
    for row in reader:
"""
"""
async def load_data(path):
  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:
      movie = {"id": row['id'], "budget": row['budget'], "revenue": row['revenue'], "popularity": row['popularity'], "release_date": row['release_date']}
      result = await db.insert_movie(movie)
      print(result)
      #return result
asyncio.run(load_data('./the-movies-dataset/movies_metadata.csv'))
"""
"""
def load_data(path):
  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:
      #movie = {"id": row['id'], "budget": row['budget'], "revenue": row['revenue'], "popularity": row['popularity'], "release_date": row['release_date']}
      #x = await db.insert_movie(movie)

      genres = ast.literal_eval(row['genres'])
      if genres:
        for genre in genres:
          print(str(genre))
          #y = await  db.insert_genre(genre)
          #z = await db.insert_movie_genre(movie, genre)

      production_companies = ast.literal_eval(str(row['production_companies']))
      if production_companies:
        for production_company in production_companies:
          print(str(production_company))
          a = await db.insert_production_company(production_company)
          b = await db.insert_production_company_movie(production_company, movie)

load_data('./the-movies-dataset/movies_metadata.csv')
"""
#movie_data = open('./the-movies-dataset/movies_metadata.csv', 'r')
#fields = movie_data.readline()
#entry = movie_data.readline()
#print(fields, entry)
"""
def date_match(value):
  pattern = '%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}'
  grok = Grok(pattern)
  result = grok.match(value)
  return result
"""