import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F

def get_stg_geo_events_cities_geo(stg__geo_events_cities_geo__path, spark):
    spark_df = spark.read \
              .option('basePath', stg__geo_events_cities_geo__path) \
              .parquet(stg__geo_events_cities_geo__path) 
    return spark_df

def add_features(df):
    df = df.select(F.col('event.message_from').alias('user_id'),
                                              F.col('event.message_ts').alias('date'),
                                              F.col('city_name'),
                                              F.col('timezone'))
    
    act_city_df = df.withColumn('act_city', 
                        F.first(F.col('city_name')).over(Window.partitionBy('user_id').orderBy(F.desc('date'))))\
                        .select(F.col('user_id'),
                        F.col('act_city'))\
                        .distinct()
    
    home_city_df = df.withColumn('lag_city', 
                           F.lag('city_name').over(Window.partitionBy('user_id').orderBy('date')))\
                            .filter((F.col('city_name')!= F.col('lag_city'))|(F.col('lag_city').isNull()))\
                            .withColumn('lag_date',
                                       F.lag('date').over(Window.partitionBy('user_id').orderBy('date')))\
                            .withColumn('diff_date', F.datediff( F.col('date'), F.col('lag_date')))\
                            .filter(F.col('diff_date')>27)\
                            .select(F.col('user_id').alias('h_user_id'), 
                                    F.col('lag_city').alias('home_city'))
    
    travel_date_df = df.withColumn('lag_city', 
                           F.lag('city_name').over(Window.partitionBy('user_id').orderBy('date')))\
                            .filter((F.col('city_name')!= F.col('lag_city'))|(F.col('lag_city').isNull()))\
                            .groupBy('user_id')\
                            .agg(F.count('city_name').alias('travel_count'), F.collect_list('city_name').alias('travel_array'))\
                            .select(F.col('user_id').alias('t_user_id'),
                                   F.col('travel_count'),
                                   F.col('travel_array'))

    local_time_df = df.withColumn('max_date', F.max('date').over(Window.partitionBy('user_id')))\
                      .filter(F.col('date')==F.col('max_date'))\
                      .withColumn('time_utc', F.date_format('max_date', "HH:mm:ss"))\
                      .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),F.col('timezone')))\
                      .select(F.col('user_id').alias('lt_user_id'), F.col('local_time'))

    result_df = act_city_df.join(home_city_df, act_city_df.user_id == home_city_df.h_user_id, 'left')\
                           .drop(F.col('h_user_id'))\
                           .select(F.col('user_id'), F.col('act_city'), F.col('home_city'))\
                           .join(travel_date_df, act_city_df.user_id==travel_date_df.t_user_id, 'left')\
                           .drop(travel_date_df.t_user_id)\
                           .join(local_time_df, act_city_df.user_id==local_time_df.lt_user_id, 'left')\
                           .drop(local_time_df.lt_user_id)
    return result_df

    

def main():
    stg__geo_events_cities_geo__path = sys.argv[1]
    dm__users_area__path = sys.argv[2]
    
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f'calculate_users_dm_{current_time}') \
        .getOrCreate()
    
    events_cities_spark_df = get_stg_geo_events_cities_geo(stg__geo_events_cities_geo__path, spark)
    dm_df = add_features(events_cities_spark_df)
    dm_df.write \
        .mode('overwrite') \
        .parquet(dm__users_area__path)
    
if __name__ == '__main__':
    main()