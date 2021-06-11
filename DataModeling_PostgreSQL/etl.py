import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ 
    This function open a single JSON file (filepath) about song_data and load it in a DataFrame
    The data is added to the dimension tables named 'songs' and 'artists' based on queries from sql_queries.py
    
    Arguments:
    - cur = cursor to the database
    - filepath = path to a JSON file on song_data
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """ 
    This function open a single JSON file (filepath) about log_data and load it in a DataFrame
    This data is added into two dimension tables named 'users' and 'time' as well as in the fact table 'songplays' based on sql_queries.py
    
    Arguments:
    - cur = Cursor to the database
    - filepath = path to the log_data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({column: data for column,data in zip (column_labels, time_data)}).dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [str(pd.to_datetime(row['ts'], unit='ms')), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    This function iterate trought a folder to create a list of string where each string is a path to a JSON file.
    For each filepath in this list, the data is processed throught the specified function (func)
    
    Arguments:
    - cur = Cursor to the database
    - conn = Connection to the database
    - filepath = path to the folder with the data that need to be processed
    - func = python-function used to insert data into the appropriated table
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    This is the Main Function of the script and will run above functions
    It first connect to the database (here it's a local database name 'postgres')
    
    It will iterate through the two data sources (song_data & log_data) to add the data into the appropriate table 
    
    NOTE: it requires to have tables already created (this can be done with the file 'create_tables.py') 
    """
    
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=pasha_enjoin_flint")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    print('\n\n-----> Data were successfully added to the database')

    conn.close()


if __name__ == "__main__":
    main()