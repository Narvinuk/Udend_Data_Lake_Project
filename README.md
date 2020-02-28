Project Desription:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Overview:

Tha main task of this project to build the ETL process to extract data from Json files stores AWS S3 storage and write into
AWS S3 as Spark parquet files using Apache spark.

Source data is set of files in Json in AWS S3 bucket
s3://udacity-dend/song_data : Contains songs and atrist data

s3://udacity-dend/log_data: contains event data of service usage e.g. who listened what song, when, where, and with which client

Data will be extracted from above raw Json files to populate following Fact and Dimension tables.

Fact Table
songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Dimension Tables

users: user info (columns: user_id, first_name, last_name, gender, level)
songs: song info (columns: song_id, title, artist_id, year, duration)
artists: artist info (columns: artist_id, name, location, latitude, longitude)
time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)
Project task flow:

Read the song data set from Json files
Extract Songs informatio from Song data set
Creat Song table data frame (song_table_df)
Read Songs data from song_table_df
Write Song_table to Parquet fie partition by year and month

Extract atrist informatin from Song table data frame (song_table_df)
Select arists data from song_table_df
Write atrist table to Parquet file.


Read log_data set from Json files
Extaract events and user data from log_data
Create log_data data frame from log_data 

Extract users information from log_data data frame where page='Nextsong'
Extract users information from log_data set to user_Table
Write users table to parquet file

Extract time daa from load_data data frame  and write into Time table

Select and write time table to parquet file partition by year and month.


Create songs play dataframe by joining artists and songs data 

Create songs_play table view
Select songs_plays table and write to Parquet file partition by year and month.


Process execution Steps:

This process has one script file etl.py

Call the etl.py script from terminal using following command:
python3 -m etl.py

Above scripts Spark sql command to read json from S3,creates data frames and writes into parquet files.

Output: Data is written to S3 output bucket as Spark parquest files.


