### Getting Started
**Requirements**
* Python v3.8.5
* PostgreSQL v13.3

**Dependencies**
* psycopg2 v2.8.6
`$ pip install psycopg2`
* pygrok v1.0.0
`$ pip install pygrok`
* wget v3.2
`$ pip install wget`

**How to Run**
Install the required modules above using the pip package installer.

PostgreSQL should be running on localhost. The file `schema.sql` can be used to create the database and tables according to the data model. In the working directory, create the file `config.py`. In this file, add the following dictionary:
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