import csv
import ast
import asyncio
import db
from pygrok import Grok

async def load_data(path):
  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:
      movie = {"id": row['id'], "budget": row['budget'], "revenue": row['revenue'], "popularity": row['popularity'], "release_date": row['release_date']}
      db.insert_movie(movie)

      genres = ast.literal_eval(row['genres'])
      if genres:
        for genre in genres:
          db.insert_genre(genre)
          db.insert_movie_genre(movie, genre)

      production_companies = ast.literal_eval(str(row['production_companies']))
      if production_companies:
        for production_company in production_companies:
          db.insert_production_company(production_company)
          db.insert_production_company_movie(production_company, movie)

#movie_data = open('./the-movies-dataset/movies_metadata.csv', 'r')
#fields = movie_data.readline()
#entry = movie_data.readline()
#print(fields, entry)