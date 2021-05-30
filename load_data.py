import csv
import ast
import asyncio
import db
from datetime import datetime
from pygrok import Grok
import re
from zipfile import ZipFile
import wget
#from s3urls import parse_url
#import boto3

# Function to download zip file from http endpoint. I was working on using Boto3 to download the file from the s3 endpoint but was running into trouble with authentication and authorization
def download_data(uri):
  file = uri.split('/')
  file = file[len(file) - 1]
  data = wget.download(uri, './data/' + file)
  return data
#download_data('https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip')

# Function to extract a specific file from the zipfile
def extract(path, file):
  with ZipFile(path, 'r') as myzip:
    data = myzip.extract(file, './data')
    return ('./' + data)
#extract('./data/the-movies-dataset.zip', 'movies_metadata.csv')

# Function to match values to a predefined grok pattern
def pattern_match(value, pattern):
  grok = Grok(pattern)
  result = grok.match(value)
  return result

# Main data pipeline function for extracting, transforming and loading data from the source data into the data model (database). Takes the http uri for the zipfile and the name of the file to extract.
async def load_data(uri, file_name):
  errlog = open("err_log.txt", "w")
  errlog.write('Start: ' + str(datetime.now()) + '\n')

  compressed = download_data(uri)
  path = extract(compressed, file_name)

  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:

      movie_id = pattern_match(str(row['id']), '%{INT:id}')
      title = re.search(r"^[0-9a-zA-Z](?:[ '.\-0-9a-zA-Z]*[0-9a-zA-Z])?$", str(row['title']))
      budget = pattern_match(str(row['budget']), '%{INT:budget}')
      revenue = pattern_match(str(row['revenue']), '%{INT:revenue}')
      popularity = pattern_match(str(row['popularity']), '%{NUMBER:popularity}')
      release = pattern_match(str(row['release_date']), '%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}')
      if release:
        release_date = '%s-%s-%s' % (release['year'], release['month'], release['day'])

      if movie_id is None or title is None or budget is None or revenue is None or popularity is None or release_date is None:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')
      else:
        movie = {"id": movie_id['id'], "title": title.group(), "budget": budget['budget'], "revenue": revenue['revenue'], "popularity": popularity['popularity'], "release_date": release_date}
        await db.insert_movie(movie)
        #print(movie)

      genres = re.search(r"(\[)(({'id': )[0-9]+(, 'name': )[ 'a-zA-Z]+(}, ))+({'id': )[0-9]+(, 'name': )[' a-zA-Z]+(}\])", str(row['genres']))
      if genres:
        genres = ast.literal_eval(genres.group())
        for genre in genres:
          await db.insert_genre(genre)
          await db.insert_movie_genre(movie, genre)
          #print(genre)
      else:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

      production_companies = re.search(r"(\[)(({'name': ).+(, 'id': )[0-9]+(})(, )?)*(({'name': ).+(, 'id': )[0-9]+(}))?(\])", str(row['production_companies']))
      if production_companies:
        if type(production_companies) != list:
          production_companies = ast.literal_eval(production_companies.group())
        for production_company in production_companies:
          #print(production_company)
          await db.insert_production_company(production_company)
          await db.insert_production_company_movie(production_company, movie)
      else:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

  errlog.write('End: ' + str(datetime.now()))
  errlog.close()
#asyncio.run(load_data('./data/movies_metadata.csv'))
asyncio.run(load_data('https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip', 'movies_metadata.csv'))

"""
def download_data(s3endpoint):
  s3info = parse_url(s3endpoint)
  s3 = boto3.client('s3')
  s3.download_file(s3info['bucket'], s3info['key'], 'the-movies-dataset.zip')
download_data('s3://com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip')
"""
"""
def load_data(path):
  genrelist = open('genres.txt', 'w')

  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    for row in reader:

      #if row['production_companies']:
        #production_companies = re.search(r"(\[)(({'name': ).+(, 'id': )[0-9]+(})(, )?)*(({'name': ).+(, 'id': )[0-9]+(}))?(\])", row['production_companies'])
      production_companies = row['production_companies']
      if production_companies:
        production_companies = ast.literal_eval(production_companies)
        genrelist.write(str(production_companies) + '\n')

      if row['genres']:
        genres = re.search(r"(\[)(({'id': )[0-9]+(, 'name': )[ 'a-zA-Z]+(})(, )?)*(({'id': )[0-9]+(, 'name': )[' a-zA-Z]+(}))?(\])", row['genres'])
      if genres:
        genres = ast.literal_eval(genres.group())
        genrelist.write(str(genres) + '\n')


      if row['title']:
        title = re.search(r"^[0-9a-zA-Z](?:[ '.\-0-9a-zA-Z]*[0-9a-zA-Z])?$", row['title'])
        print(title.group())
      else:
        title = None

  genrelist.close()

load_data('./the-movies-dataset/movies_metadata.csv')
"""
