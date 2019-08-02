import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Song_file JSON data located in a specific file path is loaded into a datframe. The data
    frame is then used to insert the data into a PostgreSQL table. The queries to insert data
    are imported from a seperate file, sql_queries.py. TRANSFORM and LOAD portions of ETL process are
    executed here.
    
    :param cur: get cursor for PostgreSQL table
    :param filepath: local file path where song_file data is stored 
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ log_file JSON data located in a specific file path is loaded into a dataframe, filtered by the
    specific action 'NextSong' and used to create time_df and user_df. The data in these dataframes are
    then inserted into PostgreSQL tables created in sql_queries.py. TRANSFORM and LOAD portions of ETL process are
    executed here.
    
    :param cur: get cursor for PostgreSQL table
    :param filepath: local file path where log_file data is stored
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong' ]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (list(t),list(t.dt.hour), list(t.dt.day), list(t.dt.weekofyear), list(t.dt.month), list(t.dt.year), list(t.dt.weekday))

    column_labels = ('timestamp','hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(list(time_data)).transpose()
    
    #me
    time_df.columns = list(column_labels)

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
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """ Given a specific filepath, this function collects the paths to all its data files(i.e., Extract portion of ETL). 
    It then passes these data files to either the process_log_file(cur, filepath) or process_song_file(cur, filepath) to complete the ETL process. 
    
    :param cur: get cursor for PostgreSQL table
    :param conn: creates connection to PostgreSQL database (must specify credentials to connect)
    :param filepath: local file paths that contain JSON data for song_file and log_file
    :param func: executes tranform and load process of data into tables created in the database we are connected to
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
        
        #extracting the cursor and datafile(i.e., filepath) and executing a specified function. That function TRANSFORMS & LOADS data.
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ Connection credentials are supplied to connect to 'sparkifydb' database in PostgreSQL and the ETL process is executed
    for all data collected in local file paths.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()