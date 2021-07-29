import configparser

"""
This sections contains all the querries used in this project
We have:
 - Querries to drop tables
 - Querries to create staging tables and final tables 
 - Querries to move data from S3 into staging tables
 - Querries to move and transform data from stagings table to final table
"""


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist              text,
                                auth                text,
                                firstName           text,
                                gender              text,
                                itemInSession       int,
                                lastName            text,
                                lenght              float,
                                level               text,
                                location            text,
                                method              text,
                                page                text, 
                                registration        float,
                                sessionId           int,
                                song                text,
                                status              int, 
                                ts                  bigint,
                                userAgent           text,
                                userId              int        )""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs           int,
                                artist_id           text,
                                artist_latitude     float,
                                artist_longitude    float,
                                artist_location     text,
                                artist_name         text,
                                song_id             text,
                                title               text,
                                duration            float,  
                                year                int         )""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int          IDENTITY(0,1), 
                                start_time  timestamp    NOT NULL,
                                user_id     text         NOT NULL, 
                                level       text,
                                song_id     text,
                                artist_id   text, 
                                session_id  int,
                                location    text,
                                user_agent  text) """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id     int   PRIMARY KEY,
                                first_name  text,
                                last_name   text,
                                gender      text, 
                                level       text) """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id     text   PRIMARY KEY,
                                title       text,
                                artist_id   text,
                                year        int,
                                duration    float) """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id   text   PRIMARY KEY, 
                                name        text, 
                                location    text, 
                                latitude    float, 
                                longitude   float) """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time  timestamp  PRIMARY KEY,
                                hour        int,
                                day         int,
                                week        int,
                                month       int,
                                year        int,
                                weekday     int) """)

# STAGING TABLES

staging_events_copy = ("""  COPY staging_events 
                            FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            REGION 'us-west-2'
                            FORMAT as json {};   """).format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""   COPY staging_songs 
                            FROM {}
                            CREDENTIALS 'aws_iam_role={}'                            REGION 'us-west-2'
                            JSON 'auto';         """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT 
                                TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
                                logs.userId,
                                logs.level,
                                songs.song_id,
                                songs.artist_id,
                                logs.sessionId,
                                logs.location,
                                logs.useragent
                                
                            FROM staging_songs AS songs
                            JOIN staging_events AS logs ON logs.song = songs.title AND logs.artist = songs.artist_name
                            WHERE logs.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT 
                                userId,
                                firstName,
                                lastName,
                                gender,
                                level
                        
                        FROM staging_events
                        WHERE page = 'NextSong'   """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year , duration)
                        SELECT DISTINCT 
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                        
                        FROM staging_songs         """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT
                                artist_id, 
                                artist_name, 
                                artist_location, 
                                artist_latitude, 
                                artist_longitude
                          
                          FROM staging_songs      """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT 
                                TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
                                DATE_PART(h,   start_time),
                                DATE_PART(d,   start_time), 
                                DATE_PART(w,   start_time),
                                DATE_PART(mon, start_time),
                                DATE_PART(y,   start_time),
                                DATE_PART(dw,  start_time)
                        
                        FROM staging_events     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]