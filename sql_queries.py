# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS Songplays"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artists"
time_table_drop = "DROP TABLE IF EXISTS Times"

# CREATE TABLES

songplay_table_create = "CREATE TABLE IF NOT EXISTS Songplays(songplay_id serial PRIMARY KEY, start_time varchar NOT NULL,\
                         user_id varchar NOT NULL, level varchar,song_id varchar, artist_id varchar,\
                         session_id varchar, location varchar, user_agent varchar);"

user_table_create = "CREATE TABLE IF NOT EXISTS Users(user_id varchar, first_name varchar, last_name varchar,\
                     gender varchar, level varchar,PRIMARY KEY(user_id));"

song_table_create = "CREATE TABLE IF NOT EXISTS Songs(song_id varchar, title varchar, artist_id varchar, year int,\
                     duration float);"

artist_table_create = "CREATE TABLE IF NOT EXISTS Artists(artist_id varchar, name varchar, location varchar, latitude int,\
                        longitude int);"

time_table_create = "CREATE TABLE IF NOT EXISTS Times(start_time varchar, hour int, day int, week int,\
                     month int, year int, weekday int,PRIMARY KEY(start_time));"

# INSERT RECORDS

songplay_table_insert = "INSERT INTO Songplays(start_time, user_id,\
                         level, song_id, artist_id, session_id, location, user_agent)\
                         VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

user_table_insert = "INSERT INTO Users(user_id, first_name, last_name, gender, level)\
                     VALUES (%s,%s,%s,%s,%s)\
                     ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level"

song_table_insert = "INSERT INTO Songs(song_id, title, artist_id, year, duration)\
                      VALUES (%s,%s,%s,%s,%s)"

artist_table_insert = "INSERT INTO Artists(artist_id, name, location, latitude, longitude)\
                      VALUES (%s,%s,%s,%s,%s)\
                      ON CONFLICT DO NOTHING"

time_table_insert = "INSERT INTO Times(start_time, hour, day, week, month,\
                     year, weekday)\
                     VALUES (%s,%s,%s,%s,%s,%s,%s)\
                     ON CONFLICT DO NOTHING"

# FIND SONGS

song_select = "SELECT song_ID, Artists.artist_ID FROM Songs JOIN Artists ON Artists.artist_ID=Songs.artist_ID \
                WHERE %s=Songs.title AND %s=Artists.name AND %s=Songs.duration;"

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]