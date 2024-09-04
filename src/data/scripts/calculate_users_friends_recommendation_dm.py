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

def add_features(df, earth_radius_km):
    message_df = df.where(f"event_type='message'").select(F.col('event.message_from').alias('from'),
                                               F.col('event.message_to').alias('to'),
                                               F.coalesce(F.col('event.message_ts'), F.col('event.datetime')).alias('ts'),
                                               F.col('lat'),
                                               F.col('lon'),
                                               F.col('date'),
                                               F.col('city_id'),
                                               F.col('city_lat'),
                                               F.col('city_lng'),
                                               F.col('timezone'),
                                               F.col('distance'))
    users_pairs = message_df.filter((F.col('from').isNotNull()) & (F.col('to').isNotNull()))\
                            .select(
                                F.col("from").alias("user"),
                                F.col("to").alias("user_right"),
                            ).distinct()\
        
    users_pairs = users_pairs.union(
                                users_pairs.select(F.col("user").alias("user_right"), 
                                     F.col("user_right").alias("user"))
                              ).distinct()
    
    
    subscription_df = df.where("event_type='subscription'")
    subscription_df = subscription_df.select(F.col('event.subscription_channel'), 
                                                F.col('event.user'))
    subscription_df = subscription_df.join(subscription_df.select(F.col("user").alias("user_right"), 
                                                            "subscription_channel"), 
                                                            "subscription_channel"
                                                              )
    window_location = Window().partitionBy('from').orderBy(F.desc('ts'))
    location_users = message_df.withColumn('rn', F.row_number().over(window_location))\
                        .filter(F.col('rn')==1)
    location_with_time = location_users.filter(F.col('timezone').isNotNull())\
                        .withColumn('time_utc', F.date_format(F.col('ts'), "HH:mm:ss"))\
                        .withColumn('local_time', F.from_utc_timestamp(F.col("time_utc"),
                                                           F.col('timezone')))\
                        .select(F.col('from').alias('user'), F.col('local_time'))
    messages_l_df = message_df.select(
                        F.col('from').alias('user'),
                        F.col('lat'), 
                        F.col('lon'),
                        F.col('date'),
                        F.col('city_id')
                        )
    messages_r_df = message_df.select(
                        F.col('from').alias('user_right'),
                        F.col('lat').alias('lat_right'), 
                        F.col('lon').alias('lon_right'),
                        F.col('date'),
                        F.col('city_id').alias('city_id_right')
                        )
    min_distance = messages_l_df.join(messages_r_df, 'date')\
            .withColumn('distance_users', F.lit(2) 
                   * F.lit(earth_radius_km) 
                   * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat'))
                                               - F.radians(F.col('lat_right')))/F.lit(2)),2)
                                   + (F.cos(F.radians(F.col('lat_right')))
                                      * F.cos(F.radians(F.col('lat')))
                                      * F.pow(F.sin((F.radians(F.col('lon'))
                                                   - F.radians(F.col('lon_right')))/F.lit(2)),2)))))\
            .filter((F.col('distance_users')<=1) & (F.col('user')<F.col('user_right')))
    result_df = min_distance\
            .join(subscription_df, on=["user", "user_right"], how="leftsemi")\
            .join(users_pairs, on=["user", "user_right"], how="leftanti")\
            .join(location_with_time, on="user", how="left")\
            .withColumn('processed_dttm', F.current_timestamp())\
            .select(
                F.col("local_time"),
                F.col("user").alias("user_left"),
                "user_right",
                F.col("processed_dttm"),
                F.col("city_id").alias("zone_id"),
            )
    return result_df

    

def main():
    stg__geo_events_cities_geo__path = sys.argv[1]
    dm__users_friends_recommendation__path = sys.argv[2]
    # Constant
    earth_radius_km = 6371
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f'calculate_users_friends_recommendation_dm_{current_time}') \
        .getOrCreate()
    
    events_cities_spark_df = get_stg_geo_events_cities_geo(stg__geo_events_cities_geo__path, spark)
    
    dm_df = add_features(events_cities_spark_df, earth_radius_km)
    dm_df.write \
        .mode('overwrite') \
        .parquet(dm__users_friends_recommendation__path)
    
if __name__ == '__main__':
    main()