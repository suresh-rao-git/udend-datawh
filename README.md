# Data Warehouse using AWS RedShift

Sparkify a music streaming startup and having data which are collected from various sources like log
files and user access.

There is need to analysize the data that is generated in order to provide further enhancements to 
Sparkify website making it more user friendly by provideing recomendation as well as to provide reports
to management to see how to make uses sticky and get more users join as members.

# Process

Data for Songs and Events are stored in S3 buckets.  This data first needs to be ETLed to RedShift as raw data without transformation.

In order to do Analytics, we need to transform the raw data in Star/Snowflake schema.  The schema is transformed into
Fact Table:
    SongPlay
Dimension tables:
    Users
    Artists
    Song
    Time

Once data is loaded in staging tables, we run ETL process to transform and move data into star schema.


# ETL Results

<h3>Staging Tables</h3>

SQL Query | Count 
----------|-------
select count(*) from staging_events_table| 8056
select count(*) from staging_songs_table|14896

<h3>Star Schema Tables</h3>

SQL Query | Count 
----------|-------
select count(*) from songs|14896
select count(*) from time|6820
select count(*) from artists|14896
select count(*) from users|6820
select count(*) from songplay|9957



# Data Queries

###Show users who have listened songs at what time

>    select u.firstName, u.lastName, a.artist_name , s.title, t.start_time 
>    from 
>        songplay as sp
>    join users as u on ( u.user_id = sp.user_id )
>    join artists as a on ( a.artist_id = sp.artist_id )
>    join song as s on ( s.song_id = sp.song_id )
>    join time as t on ( t.start_time = sp.start_time )



