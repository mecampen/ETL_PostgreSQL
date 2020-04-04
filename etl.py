import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    -opens the song file
    -inserts the song data in the table 'Songs'
    -inserts the artist data in the table 'Artists'
    """
    df = pd.read_json(filepath,typ='series') #typ='series': read the json file as a one dimensional array

    song_data =[df.song_id, df.title, df.artist_id, df.year, df.duration]
    cur.execute(song_table_insert, song_data)
    
    artist_data = [df.artist_id, df.artist_name, df.artist_location, df.artist_latitude, df.artist_longitude]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    -opens the log file
    -filters out all the logs without the action being 'NextSong' 
    -converts timestamp column to datetime format
    -creates a dataframe containing all the time data and inserts the data to the Table 'Times'
    -copy the user data from the log data and drop all duplicate rows
    -insert the user data to the Table 'Users'
    -selects data about played songs and inserts it into the Table 'Songplays' 
    """
    df = pd.read_json(filepath, lines=True) #lines=True: per row one json object

    is_NextSong = df['page']=='NextSong'
    df = df[is_NextSong]

    df['ts']=pd.to_datetime(df['ts'],unit='ms')
    
    time_data = [ df.ts.dt.time, df.ts.dt.hour, df.ts.dt.day, df.ts.dt.week, df.ts.dt.month, df.ts.dt.year, df.ts.dt.weekday]
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    
    time_dict=dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df_with_duplicates = df[['userId', 'firstName', 'lastName','gender','level']].copy()
    user_df = user_df_with_duplicates.drop_duplicates()

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    -uses os to find all files in the used filepath with .json ending
    -gets the number of found files and prints it to the terminal
    -stores all filepaths in the list all_files
    -uses the argument function to process the data
    -every use of the function is printed to the terminal
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    -connects to the database
    -uses the function process_data() twice:
        1. with the song_data files and the specified function for that case: process_song_file()
        2. with the log_data files and the specified function for that case: process_log_file()
    -closes the connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()