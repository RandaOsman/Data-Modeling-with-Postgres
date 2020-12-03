A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis.

Schema:
Schema for Song Play Analysis

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table

    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Dimension Tables

    users (user_id, first_name, last_name, gender, level)

    songs (song_id, title, artist_id, year, duration)

    artists (artist_id, name, location, latitude, longitude)

    time - timestamps of records in songplays broken down into specific units

    (start_time, hour, day, week, month, year, weekday)

steps:

1. runt the create_tables.py 

to establish the DB connection to Sparkfy DB and and calling the  sql_queries.py


2. prepare the sql_queries.py 

to include the create statment,insert statments for the all tables and the select query from the fact table  

3. run create_tables.py again

4. runt ETL.py to perform ETL read the  json files and load them into each table to start

it is performed on song_data data set to fill the songs and artists dimensional tables,
log_data data set, will feed the time and users dimensional tables and the songplays fact table.

2. load the data into tables (songs,artists,time,users,songplays)

