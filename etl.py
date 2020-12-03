import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import re


"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """

def process_song_file(cur, filepath):
    # insert song record
    # open song file
    df = pd.read_json(filepath, lines=True)
    for value in df.values:
        song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    df=pd.read_json(filepath,lines=True)
    for artist in df.values:
        artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)
    
    
"""
    This procedure processes log files whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the time table.
    also extracts the users information in order to store it into the user table 
    moreover storing the necessary data into fact table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """    

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)
#     df.head()
    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data =[t, t.dt.hour, t.dt.day , t.dt.weekofyear, t.dt.month ,t.dt.year, t.dt.weekday]
    column_labels = ['timestamp','hour', 'day', 'weekofyear', 'month','year', 'weekday']
    
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
#     test = list(time_df[['timestamp', 'hour', 'day', 'weekofyear', 'month','year','weekday']].values[0])

    for i, row in time_df.iterrows(): 
        cur.execute(time_table_insert,list(row))
        

    
    # load user table
    df=pd.read_json(filepath,lines=True)
    for value in df.values:
        user_df = list(df[["userId", "firstName", "lastName", "gender", "level"]].values[0])
        
    # insert user records
#     for i, row in user_df.iterrows():
        cur.execute(user_table_insert, user_df)
        
        

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length ,row.artist ))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        
        
#         no_newline = (x.replace('', '0') for x in songplay_data)
#         test = [x for x in no_newline if x]
        
#         test = [re.sub("[:\-() ]","NA",x) for x in songplay_data]
        
#         test= songplay_data.replace(r'\([^()]*\)', '', regex=True)
        
        cur.execute(songplay_table_insert, songplay_data)


"""
Use the num_files function to get a number of all JSON files 

    INPUTS: 
    * getting number of files for the provided filepath
    * loop on the number of files
    """
       
        
def process_data(cur, conn, filepath, func):
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
        
    """   
    Define the DB connection by providing the Host , db name , id and passord
    
    also define the CUR variable .

    INPUTS: 
    * DB (infor) for CONN
    * filepath for the process_data
    """    


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()