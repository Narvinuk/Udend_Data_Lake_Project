import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col , monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
      """Reads data file containing the song metadata and writes songs_table and artists_table parquets files in S3.
    Keyword arguments:
    spark -- the spark session
    input_data -- the input file directory in S3
    output_data -- the output parquet file directory in S3
    """
    # get filepath to song data file
    print("Reading song data file..")
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df_sd = spark.read.json(song_data)
    print("Song_data schema:")
    df_sd.printSchema()

    # extract columns to create songs table
    df_sd.createOrReplaceTempView("song_table_df")
    songs_table = spark.sql ("""
                                SELECT Distinct song_id, title, artist_id, year, duration
                                FROM song_table_df
                                ORDER BY song_id
                                """) 
    print("Songs_table examples:")
    songs_table.show(5)
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table parquet files...")
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs_table")
    
    print("Completed writing songs_table parquet files...")
    # extract columns to create artists table
    print("extract columns to create artists table")
    artists_table = spark.sql("""
                                SELECT Distinct   artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                                FROM song_table_df
                                ORDER BY artist_id
                                """)
    print("artist table example")
    artists_table.show(5)
    
    # write artists table to parquet files
    
    print("Writing artist_table parquet files...")
        
    artists_table.write.mode("overwrite").parquet(output_data+"artist_table")
    
    print("Completed writing artist_table parquet files...")

def process_log_data(spark, input_data, output_data):
    
     """Reads data file containing the logs from user activity and loads into parquet files for time_table, users_table and songplays_table in S3.
    Keyword arguments:
    spark -- the spark session
    input_data -- the input file directory in S3
    output_data -- the output parquet file directory in S3
    """

    # get filepath to log data file
    log_data =input_data+"log_data/*/*/*.json"

    # read log data file
    print ("Reading log data file...")
    df_ld = spark.read.json(log_data)
    print("log_data schema..")
    df_ld.printSchema()
    
    # filter by actions for song plays
    print ("filter by actions for song plays..")
    df_ld_NextSong = df_ld.where(df_ld.page == "NextSong")
    df_ld_NextSong.createOrReplaceTempView("log_table_df")

    # extract columns for users table    
    print ("extract columns for users table")
    user_table = spark.sql("""
                             SELECT Distinct userId,firstName,lastName,gender,level
                             FROM log_table_df
                             ORDER BY lastName
                            
                            """)
    print ("user_table example...")
    user_table.show(5)
    
    # write users table to parquet files
    print ("write users table to parquet files....")       
    user_table.write.mode("overwrite").parquet(output_data+"user_table")

    # create timestamp column from original timestamp column
    print ("Create timestamp column from ts...")
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    #from pyspark.sql.types import TimestampType
    #get_timestamp =udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df_ld_NextSong = df_ld_NextSong.withColumn("timestamp", get_timestamp("ts"))
    print("print schema after timestamp conversion")       
    df_ld_NextSong.printSchema()
    print ("print sample data .....")       
    df_ld_NextSong.show(5)
    
    # create datetime column from original timestamp column
    print("create datetime column from original timestamp column...")       
    print("Creating datetime column...")
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')
    df_ld_NextSong =df_ld_NextSong.withColumn('starttime',get_datetime('ts'))
    
    print ("Print schema...")
           
    df_ld_NextSong.printSchema()
    print ("print sample data....")
    df_ld_NextSong.show(5)
    
    # extract columns to create time table
    print("Create View")
    df_ld_NextSong.createOrReplaceTempView("time_table_df")
    print("select table")
    time_table = spark.sql("""
                            SELECT DISTINCT starttime  as start_time,
                                            hour(timestamp) as hour,
                                            day(timestamp) as day,
                                            weekofyear(timestamp) as week,
                                            month(timestamp) as month,
                                            year(timestamp) as year,
                                            dayofweek(timestamp) as weekday
                            FROM time_table_df
                            ORDER BY start_time
                            
                            """)
    print ("Printing Time table schema...")
    time_table.printSchema()
    print ("Printing Time table example...")
    time_table.show(5)
    # write time table to parquet files partitioned by year and month
    print ("creating time_table  to parquet files partitioned by year and month...")
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time_table")
    print ("completed time_table  to parquet files partitioned by year and month...")
    
    # read in song data to use for songplays table
    song_data = input_data + "song-data/A/A/*/*.json"
    print("Reading Song data from Json files")
    df_sd = spark.read.json(song_data)
    print("Song schema")
    df_sd.printSchema()
    # extract columns from joined song and log datasets to create songplays table 
    print("Joining song and log datasets")
    songplays_table = df_ld_NextSong.join(df_sd,(df_ld_NextSong.artist==df_sd.artist_name) & (df_ld_NextSong.song==df_sd.title))
    print("Completed joining")
    print("print songplay schema")
    songplays_table.printSchema()
    print("create songplay_id field with autoincremental value")
    songplays_table=songplays_table.withColumn("songplay_id",monotonically_increasing_id())
    print ("songplay schema after new column added")
    songplays_table.printSchema()
    songplays_table.createOrReplaceTempView("songplays_df")
    songplays_table =spark.sql("""
                                SELECT songplay_id,
                                       timestamp as start_time,
                                       year(timestamp) as year,
                                       month(timestamp) as month,
                                       userId as user_id,
                                       level as level,
                                       song_id as song_id,
                                       artist_id as artist_id,
                                       sessionId as session_id,
                                       location as location,
                                       userAgent as user_agent
                                FROM songplays_df
                                ORDER BY (user_id,session_id)
                                """)
    print ("printing songplays table schema")
    songplays_table.printSchema()    
    print ("songplays example data")
    songplays_table.show(5)    

    # write songplays table to parquet files partitioned by year and month
    print("Creating songplays table parquet file..")
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    """This program will extract all files from song and log directories in S3, load the schema-on-read tables, perform data transformations and load into parquet files in S3 data lake.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nv-dend-spark-prj/song-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
