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

# Function to extract a specific file from the zipfile
def extract(path, file):
  with ZipFile(path, 'r') as myzip:
    data = myzip.extract(file, './data')
    return ('./' + data)

# Function to match values to a predefined grok pattern
def pattern_match(value, pattern):
  grok = Grok(pattern)
  result = grok.match(value)
  return result

# Main data pipeline function for extracting, transforming and loading data from the source data into the data model (database). Takes the http uri for the zipfile and the name of the file to extract.
async def load_data(uri, file_name):

  # Open an error log file
  errlog = open("err_log.txt", "w")
  errlog.write('Start: ' + str(datetime.now()) + '\n')

  # Download the zipfile
  compressed = download_data(uri)
  # Extract the specific file we need
  path = extract(compressed, file_name)

  # Open the csv file and read it using csv.DictReader class, which maps each row to a dictionary whose keys correspond to the csv field names
  with open(path, newline='') as movie_data:
    reader = csv.DictReader(movie_data)
    # Each row corresponds to a single movie
    for row in reader:
      # For each movie, access the data to be loaded into the data model by field name, and validate each piece of data using a grok pattern or regular expression
      movie_id = pattern_match(str(row['id']), '%{INT:id}')
      title = re.search(r"^[0-9a-zA-Z](?:[ '.\-0-9a-zA-Z]*[0-9a-zA-Z])?$", str(row['title']))
      budget = pattern_match(str(row['budget']), '%{INT:budget}')
      revenue = pattern_match(str(row['revenue']), '%{INT:revenue}')
      popularity = pattern_match(str(row['popularity']), '%{NUMBER:popularity}')
      release = pattern_match(str(row['release_date']), '%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}')
      if release:
        release_date = '%s-%s-%s' % (release['year'], release['month'], release['day'])

      # If any piece of movie data did not pass through the validation step, write the current row of data to the error log for further examination and processing later
      if movie_id is None or title is None or budget is None or revenue is None or popularity is None or release_date is None:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')
      # If the the movie data passes the validation step, insert that data into the movies database
      else:
        movie = {"id": movie_id['id'], "title": title.group(), "budget": budget['budget'], "revenue": revenue['revenue'], "popularity": popularity['popularity'], "release_date": release_date}
        await db.insert_movie(movie)

      # For each movie, access the genres associated with that movie by field name, and validate the genres data using regular expressions
      genres = re.search(r"^(\[)(({'id': )[0-9]+(, 'name': )[ 'a-zA-Z]+(})(, )?)*(({'id': )[0-9]+(, 'name': )[' a-zA-Z]+(}))?(\])$", str(row['genres']))
      # If the genre data passes the validation step, insert each genre into the movies database, and insert a link between each genre and the current movie
      if genres:
        genres = ast.literal_eval(genres.group())
        for genre in genres:
          await db.insert_genre(genre)
          await db.insert_movie_genre(movie, genre)
      # If the genre data doesn't pass the validation step, write the current row to the error log
      else:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

      # For each movie, access the production companies associated with the current movie by field name, and validate the production companies data using regular expressions
      production_companies = re.search(r"^(\[)(({'name': ).+(, 'id': )[0-9]+(})(, )?)*(({'name': ).+(, 'id': )[0-9]+(}))?(\])$", str(row['production_companies']))
      # If the production companies data passes the validation step, insert each production company into the movies database, and insert a link between each production company and the current movie
      if production_companies:
        if type(production_companies) != list:
          production_companies = ast.literal_eval(production_companies.group())
        for production_company in production_companies:
          await db.insert_production_company(production_company)
          await db.insert_production_company_movie(production_company, movie)
      # if the production companies data doesn't pass the validation step, write the current row to the error log
      else:
        errlog.write(str(datetime.now()) + ' - ' + str(row) + '\n')

  # After reading the entire .csv file, close the error log file
  errlog.write('End: ' + str(datetime.now()))
  errlog.close()

# Run load_data script with http endpoint for the-movies-dataset.zip and specifying movies-metadata.csv
asyncio.run(load_data('https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip', 'movies_metadata.csv'))

"""
# Attempt at retrieving zipfile from s3 bucket
def download_data(s3endpoint):
  s3info = parse_url(s3endpoint)
  s3 = boto3.client('s3')
  s3.download_file(s3info['bucket'], s3info['key'], 'the-movies-dataset.zip')
download_data('s3://com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip')
"""
