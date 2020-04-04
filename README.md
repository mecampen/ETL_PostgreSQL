# ETL process for Song Play Analysis using PostgreSQL

The used dataset is the million songs dataset (http://millionsongdataset.com/). Also log data was created using this event simulator (https://github.com/Interana/eventsim) that fits to the song data. To run this scripts it is mandatory to have psycopg2 installed. 
The database created has a star schema for easy access and fast queries. These steps are performed by this programm.

    1. creating the database with all the needed tables. (star schema tables)
    2. loading data from json/csv files into a pandas dataframe
    3. insert data from the dataframes to the database

sql_queries.py stores all queries we want to make to the database including table creation, doing inserts and select songs
with the specific tables: Songpplays, Users, Songs, Artists, Times

create_tables.py connects to the server and uses the in sql_queries defined creation queries to build the database. Furthermore
it deletes all old tables before creating new ones to start a fresh session.

in etl.ipynb are various data format manipulations utilizing pandas to make the data easy usable in postgreSQL.
all the dimension tables and the fact table are bulid. This notebook is just for test and development purposes. It is not needed for the actual etl process.
in etl.py is the Jupyter Notebook translated to a Python script which when run creates the database with the right entries for
each table out the data sources.

The ETL process can be run following these steps:
    
    1. run 'python create_tables.py' in the terminal
    2. run 'python etl.py' in the terminal
