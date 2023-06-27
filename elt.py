import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,desc, to_timestamp,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import types as T



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
    """
    getting songs from s3 process them to proper data structure save them back to s3
    """
    
    # get filepath to song data file
    song_data = input_data+'/song-data/*/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df = spark.sparkContext.parallelize(df)
    
    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title',' artist_id', 'year', 'duration').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet('{}/songs_table'.format(output_data))

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id',
                                  'artist_name as name', 
                                  'artist_location as location', 
                                  'artist_latitude as latitude',
                                  'artist_longitude as longitude').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet('{}/artists_table'.format(output_data))

def process_log_data(spark, input_data, output_data):
    """
    getting log from s3 process them to proper data structure save them back to s3
    """
    # get filepath to log data file
    log_data = input_data+'/log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page=='NextSong'")
    
    df = spark.sparkContext.parallelize(df)
    
    
    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet('{}/users_table'.format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000)
    df = df.withColumn('time_stamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), T.TimestampType()) 
    df = df.withColumn("datetime", get_datetime('time_stamp'))
    
    # extract columns to create time table
    time_table = (df.selectExpr('datetime as start_time ')
                    .withColumn('hour',hour(col('start_time')))
                    .withColumn('day',dayofmonth(col('start_time')))
                    .withColumn('week',weekofyear(col('start_time')))
                    .withColumn('month',month(col('start_time')))
                    .withColumn('year',year(col('start_time')))
                    .withColumn('weekday',dayofweek(col('start_time')))
                 )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet('{}/time_table'.format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('{}/songs_table'.format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(song_df,df.song == song_df.title,'inner').selectExpr('datetime as start_time',
                                                                                   'userId as user_id',
                                                                                   'level',
                                                                                   'song_id',
                                                                                   'artist_id',
                                                                                   'sessionId as session_id',
                                                                                   'location',
                                                                                   'userAgent as user_agent' )
                       .withColumn('songplay_id',monotonically_increasing_id())
                       .withColumn('year',year('start_time'))
                       .withColumn('month',month('start_time'))

                      )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet('{}/songplays_table'.format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://testing666/output_data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
