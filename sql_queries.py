# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists  users"
song_table_drop = "drop table  if exists songs"
artist_table_drop = "drop table if exists  artists"
time_table_drop = "drop table if exists  time"

# CREATE TABLES -- note for review PK means unique adn not null

songplay_table_create = (""" 
create table if not exists songplays(
songplay_id serial primary key,
start_time timestamp NOT NULL,
user_id varchar NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
)
""")

user_table_create = (""" 
create table if not exists users(
user_id varchar primary key,
first_name varchar,
last_name varchar,
gender char(1),
level varchar
)
""")

song_table_create = ("""
create table if not exists songs (
song_id varchar primary key,
title varchar,
artist_id varchar, 
year int,
duration numeric
)
""")

artist_table_create = ("""
create table if not exists artists (
artist_id varchar primary key,
artist_name varchar,
artist_location varchar,
artist_latitude numeric,
artist_longitude numeric
)
""")

time_table_create = ("""
create table if not exists time (
start_time timestamp(6) WITHOUT TIME ZONE primary key,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = (""" insert into songplays (start_time , user_id , level , song_id , artist_id , session_id , location , user_agent ) VALUES (to_timestamp(%s::text, 'YYYYMMDDHH24MISS'),%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (songplay_id) DO NOTHING 
""")

user_table_insert = (""" insert into users (user_id , first_name , last_name , gender , level ) VALUES (%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = (""" insert into songs(song_id,title,artist_id,year,duration ) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING 
""")


artist_table_insert = ("""insert into artists (artist_id,artist_name, artist_location, artist_latitude, artist_longitude ) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING 
""")

time_table_insert = (""" insert into time (start_time, hour , day , week , month , year , weekday ) VALUES (%s,%s,%s,%s,%s ,%s,%s) ON CONFLICT (start_time) DO NOTHING 
""")

# FIND SONGS

song_select = (""" 
select 
s.song_id , a.artist_id  
from songs s join artists a 
on s.artist_id=a.artist_id 
where s.title=%s and s.duration=%s and a.artist_name=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]