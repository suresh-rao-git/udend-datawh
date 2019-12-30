import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events_table;"
staging_songs_table_drop = "drop table if exists staging_songs_table;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_songs_table_create= ("""
    create table if not exists staging_songs_table ( 
        num_songs integer ,
        artist_id varchar,
        artist_latitude decimal(13,8),
        artist_lontitude decimal(13,8),
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar ,
        duration decimal(13,8),
        year integer
    )
""")


staging_events_table_create= ("""
    create table if not exists staging_events_table( 
        artist_name varchar ,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length decimal(13,8),
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId varchar
    );
""")


songplay_table_create = ("""
    create table if not exists songplay (
        songplay_id integer identity (0,1) not null, 
        start_time timestamp not null , 
        user_id integer not null, 
        level varchar , 
        song_id varchar not null , 
        artist_id varchar not null sortkey distkey , 
        session_id integer , 
        location varchar , 
        user_agent varchar 
    );
""")

user_table_create = ("""
    create table if not exists users ( 
        userid integer primary key sortkey , 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    ) diststyle all;
""")

song_table_create = ("""
    create table if not exists songs (
        song_id varchar primary key sortkey, 
        title varchar , 
        artist_id varchar not null, 
        year integer , 
        duration decimal(13,8)
    ) diststyle all;
""")

artist_table_create = ("""
	create table if not exists artists ( 
        artist_id varchar primary key sortkey , 
        name varchar, 
        location varchar, 
        latitude decimal(10,5) , 
        longitude  decimal(10,5)
    ) diststyle all;
""")

time_table_create = ("""
    create table if not exists time (
        start_time timestamp primary key sortkey, 
        hour integer, 
        day integer , 
        week integer , 
        month integer , 
        year integer , 
        weekday varchar 
    ) diststyle all;
""")

# STAGING TABLES



staging_events_copy = ("""
    copy staging_events_table from {} 
    credentials 'aws_iam_role={}' 
    format as json {}
    compupdate off 
    region 'us-west-2';
""").format( config.get( 'S3','LOG_DATA'), 
              config.get('IAM_ROLE', 'ARN'),
              config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""
    copy staging_songs_table from {} 
    credentials 'aws_iam_role={}' 
    format as json 'auto'
    compupdate off 
    region 'us-west-2';
""").format( config.get( 'S3','SONG_DATA'), 
              config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplay ( start_time , user_id , level , song_id , artist_id ,
        session_id , location , user_agent )
        select distinct 
            TIMESTAMP 'epoch' + set.ts/1000  * interval '1 second' as start_time,
            set.userId::integer as user_id, 
            set.level as level, 
            sst.song_id as song_id, 
            sst.artist_id as artist_id,
            set.sessionId as session_id , 
            set.location as location, 
            set.userAgent as user_agent
        from staging_events_table set , staging_songs_table as sst
        where 
            set.artist_name = sst.artist_name 
            and set.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users ( userid, first_name, last_name, gender, level) 
    select distinct
        set.userId::integer as userId, 
        set.firstName as first_name, 
        set.lastName as last_name,
        set.gender as gender,
        set.level as level
    from staging_events_table as set
    where set.page = 'NextSong' ;
""")

song_table_insert = ("""
    insert into songs ( song_id, title, artist_id, year, duration ) 
    select distinct
        song_id , 
        title, 
        artist_id, 
        year, 
        duration 
    from staging_songs_table ;
""")

artist_table_insert = ("""
    insert into artists (artist_id , name, location, latitude, longitude ) 
    select distinct
        artist_id , 
        artist_name as name , 
        artist_location as location,
        artist_latitude as latitude,  
        artist_lontitude as longitude
    from staging_songs_table ;
""")

time_table_insert = ("""
    	insert into time( start_time, hour, day, week, month, year, weekday ) 
        select distinct TIMESTAMP 'epoch' + set.ts/1000  * interval '1 second' as start_time,
        extract( hour from start_time ) as hour,
        extract( day from start_time ) as day , 
        extract( week from start_time) as week ,        
        extract( month from start_time) as month ,
        extract( year from start_time ) as year , 
        extract ( weekday from start_time) as weekday
        from staging_events_table as set
        where set.page = 'NextSong' ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
