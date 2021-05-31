## Getting Started
### Requirements
* Python v3.8.5
* PostgreSQL v13.3

### Dependencies
* psycopg2 v2.8.6
`$ pip install psycopg2`
* pygrok v1.0.0
`$ pip install pygrok`
* wget v3.2
`$ pip install wget`

### How to Run
Install the required modules above using the pip package installer.

PostgreSQL should be running on `localhost`. The file `schema.sql` can be used to create the database and tables according to the data model. In the working directory, create the file `config.py`. In this file, add the following dictionary:
```
config = {
  "host": "localhost",
  "database": "movies",
  "user": "your_username",
  "password": "your_password"
}
```
where "user" and "password" is the information for the role that created the database or that has the proper privileges for access.

In the file `load_data.py` make sure the function call to the `load_data` function is not commented out. It should look like `asyncio.run(load_data('https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip', 'movies_metadata.csv'))`, where the first argument is the uri for `the-movies-dataset.zip` and the second argument is the name of the file `movies_metadata.csv`. `load_data.py` can then be run from the command line in the working directory: `$ python3 load_data.py`.

Once that script has finished, in the file `output_data.py` make sure the function call to the `generate_all_outputs` function is not commented out. Then to generate .csv files for each of the 9 questions posed in the `README`, run `output_data.py` from the command line in the working directory: `$ python3 output_data.py`.

## Comments
The SQL query for gathering `Movie Genre Details: revenue by genre by year` with my data model can be found in `db.py` in the function `select_revenue_by_genre_by_year`.

The directory `question outputs` contains the output .csv files from running the scripts `load_data.py` and `output_data.py`. The file `error_log.txt` contains the error log from running `load_data.py`.

I have so far been unable to connect to the s3 endpoint `s3://com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip` due to authentication/authorization issues, so in my code I retrieve `the-movies-dataset.zip` from the http endpoint `https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip`. I set up the AWS CLI to access my own AWS resources, but receive the error `botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden` when accessing the Guild resource. I should have reached out about this sooner, but I left this step until later in my process.

If I were to continue improving this project, my next step would be to re-examine the rows that are being written to my error log, and identifying the things that are causing them to fail parts of my pattern-matching data validation. I think that a major cause is that a missing or incorect character in the right place can throw off the entire dictionary created for a row through `csv.DictReader`, causing values to be associated with the wrong field names. While it was a convenient starting place to look up values by field name, moving forward I would write more robust regular expressions to look at the entire string for a row and match the specific values I am extracting. I suspect that the regex I wrote for checking the genre and production companies lists would function if passed the entire row string, while some of the other values that don't have as distinct a structure would require a bit more thought.

I would also ask about how to handle rows with incomplete data, as well as rows with `0` budget or revenue, or `[]` genres or production companies. Should those movies be included in the query results or not? Should those movies be stored in the database or not?