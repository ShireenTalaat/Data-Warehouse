import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

drop_table_queries=[staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,\
                    song_table_drop, artist_table_drop, time_table_drop]



# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar,
                                                                            auth  varchar,
                                                                            firstName varchar, 
                                                                            gender varchar,
                                                                            itemInSession integer,
                                                                            lastName  varchar,
                                                                            length float,
                                                                            level varchar,
                                                                            location varchar,
                                                                            method varchar,
                                                                            page varchar,
                                                                            registration  bigint,
                                                                            sessionId integer,
                                                                            song varchar, 
                                                                            status integer,
                                                                            ts timestamp , 
                                                                            userAgent varchar,
                                                                            userId  integer
)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (song_id  varchar,
                                                                            num_songs integer,
                                                                            title varchar,
                                                                            artist_name varchar,
                                                                            artist_latitude  float,
                                                                            year integer,
                                                                            duration float, 
                                                                            artist_id varchar,
                                                                            artist_longitude float,
                                                                            artist_location  varchar
)""")



songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id  bigint      identity(0,1),
                                                                   start_time  timestamp   REFERENCES time(start_time),
                                                                   user_id       integer   REFERENCES users (user_id),
                                                                   level         varchar   NOT NULL,
                                                                   song_id       varchar   REFERENCES songs (song_id),
                                                                   artist_id     varchar   REFERENCES artists(artist_id),
                                                                   session_id    integer   NOT NULL, 
                                                                   location      varchar   NOT NULL, 
                                                                   user_agent    varchar   NOT NULL

)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id    integer PRIMARY KEY,
                                                          first_name varchar,
                                                          last_name  varchar,
                                                          gender     varchar,
                                                          level      varchar 

)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id   varchar PRIMARY KEY, 
                                                          title     varchar,
                                                          artist_id varchar ,
                                                          year      integer,
                                                          duration  decimal

)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id        varchar PRIMARY KEY,
                                                              artist_name      varchar,
                                                              location         varchar,
                                                              artist_latitude  float, 
                                                              artist_longitude float
)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY,
                                                         hour       integer,
                                                         day        integer,
                                                         week       integer,
                                                         month      integer,
                                                         year       integer,
                                                         weekday    integer
)""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} 
                            iam_role '{}'
                            format as json {} ;
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""copy staging_songs from {} 
                         iam_role '{}'
                         format as json 'auto' ;
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT   TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' as start_time,
         events.userId,
         events.level,
         songs.song_id,
         songs.artist_id,
         events.sessionId,
         events.location,
         events.userAgent
FROM staging_events AS events
JOIN staging_songs AS songs
     ON (events.artist = songs.artist_name)
     AND (events.song = songs.title)
     AND (events.length = songs.duration)
     WHERE events.page = 'NextSong';
""")


user_table_insert = ("""INSERT INTO users 
SELECT DISTINCT userId,  
                firstName,
                lastName,
                gender,
                level      
                
FROM staging_events 
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs 
SELECT DISTINCT song_id,
                title  ,
                artist_id,
                year,
                duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists 
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
                
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT '1970-01-01'::date + (ts/1000) * interval '1 second' AS start_time,
       EXTRACT(HOUR FROM start_time) As hour,
       EXTRACT(DAY FROM start_time) As day,
       EXTRACT(WEEK FROM start_time) As week,
       EXTRACT(MONTH FROM start_time) As month,
       EXTRACT(YEAR FROM start_time) As year,
       EXTRACT(DOW FROM start_time) As weekday

FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]