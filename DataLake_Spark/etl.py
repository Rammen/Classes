import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']      = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']  = config['AWS']['AWS_SECRET_ACCESS_KEY']

input_path   = config['PATH']['INPUT_PATH']
output_path  = config['PATH']['OUTPUT_PATH']


def create_spark_session():
    """
    This function connect to an apache spark session.
    """
    
    spark = SparkSession \
                .builder \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                .getOrCreate()
    
    print('\n\n    ----> Spark Session Created\n\n')

    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function take the songs data from S3, transform them with spark and save them in S3.
    Based on the songs file we create 2 dimension tables:
    - Songs table
    - Artist table
    
    Argument:
    - spark : spark session
    - input_data : location of the raw data (here S3)
    - output_data : location to save the data (here S3)
    """
    print('\n\n    ----> Reading SONG data from S3\n\n')
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # Create a temporary view to use later (will be used to create fact table)
    df.createOrReplaceTempView('songs')
    
    """
    CREATE TABLE DIM 1. SONGS
    """

    print('\n\n    ----> Creating SONGS Table')
    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
                    .dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data, 'songs', 'songs.parquet'))


    print('    ----> SONGS Table Created\n\n')
    
    """
    CREATE TABLE DIM 2. ARTISTS
    """

    print('\n\n    ----> Creating ARTISTS Table')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists', 'artists.parquet'))
       
    print('    ----> ARTISTS Table Created\n\n')



def process_log_data(spark, input_data, output_data):
    """
    This function take the logs from S3, transform them with spark and save them in S3.
    Based on the logs file we create 2 dimension tables:
    - Users table
    - Time table
    
    In this function, we also create the fact tables (songs_plays). 
    
    Argument:
    - spark : spark session
    - input_data : location of the raw data (here S3)
    - output_data : location to save the data (here S3)
    """

    print('\n\n    ----> Reading LOG data from S3\n\n')
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "Next Song")
    
    # Create a unique ID for songplay
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    
    
    """
    CREATE DIM TABLE 3. USERS
    """
    
    print('\n\n    ----> Creating USERS Table')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]) \
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName','last_name') \
                    .dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users', 'users.parquet'))

    print('    ----> USERS Table Created\n\n')


    """
    CREATE DIM TABLE 4. TIME
    """
    
    print('\n\n    ----> Creating TIME Table')
    
    # create timestamp column from original timestamp column (unix ms -> timestamp)
    # using UDF (User Defined Function)
    get_timestamp = udf(lambda x: from_unixtime(x), TimestampType())
    
    df = df.withColumn('start_time', get_timestamp(df.ts))
   
    # create the base time_table and remove any duplicate date or/and empty values
    time_table = df.select(['start_time']) \
                   .dropna() \
                   .dropDuplicates(["start_time"])
    
    # add specific date columns within the time_table
    time_table = time_table.withColumn('year', year('start_time')) \
                           .withColumn('month', month('start_time')) \
                           .withColumn('week', weekofyear('start_time')) \
                           .withColumn('weekday', dayofweek('start_time')) \
                           .withColumn('day', dayofmonth('start_time')) \
                           .withColumn('hour', hour('start_time'))
                                       
    print('    ----> TIME Table Created\n\n')
                          
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, 'time', 'time.parquet'))
    
    # Create a temporary views to create fact table and time table
    df.createOrReplaceTempView('logs')
    time_table.createOrReplaceTempView('times')
                        
    """
    CREATE FACT TABLE: SONGPLAYS
    """
    # extract columns from joined song and log datasets to create songplays table 
    
    print('\n\n    ----> Creating SONGPLAYS Table')

    songplays_table = spark.sql(
        """
        SELECT logs.songplay_id, logs.start_time, logs.userId AS user_id, logs.level, songs.song_id, songs.artist_id,
               logs.sessionId AS session_id, logs.location, logs.userAgent AS user_agent, times.year, times.month
        FROM logs
        JOIN songs ON songs.artist_name=logs.artist AND songs.title=logs.song
        JOIN times ON times.start_time=logs.start_time
        ORDER BY start_time    
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, 'songplays', 'songplays.parquet'))

    print('    ----> SONGPLAYS Table Created\n\n')

def main():
    spark = create_spark_session()
    input_data = input_path
    output_data = output_path
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    
if __name__ == "__main__":
    main()
