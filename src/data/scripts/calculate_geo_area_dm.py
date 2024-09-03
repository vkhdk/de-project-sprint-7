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
    window_week = Window().partitionBy('week')
    window_month = Window().partitionBy('month')
    window_week_tz = Window().partitionBy('week', 'city_id')
    window_month_tz = Window().partitionBy('month', 'city_id')
    window = Window().partitionBy('event.message_from').orderBy(F.col('date'))
    result_df = df \
        .withColumn("month",F.trunc(F.col("date"), "month"))\
        .withColumn("week",F.trunc(F.col("date"), "week"))\
        .withColumn("rn",F.row_number().over(window))\
        .withColumn("week_message",F.sum(F.when(df.event_type == "message",1).otherwise(0)).over(window_week_tz))\
        .withColumn("week_reaction", F.sum(F.when(df.event_type=="reaction", 1).otherwise(0)).over(window_week))\
        .withColumn("week_subscription", F.sum(F.when(df.event_type=="subscription", 1).otherwise(0)).over(window_week))\
        .withColumn("week_user", F.sum(F.when(F.col("rn")==1, 1).otherwise(0)).over(window_week_tz))\
        .withColumn("month_message",F.sum(F.when(df.event_type == "message",1).otherwise(0)).over(window_month_tz))\
        .withColumn("month_reaction", F.sum(F.when(df.event_type=="reaction", 1).otherwise(0)).over(window_month))\
        .withColumn("month_subscription", F.sum(F.when(df.event_type=="subscription", 1).otherwise(0)).over(window_month))\
        .withColumn("month_user", F.sum(F.when(F.col("rn")==1, 1).otherwise(0)).over(window_month_tz))\
        .select(F.col("month"),
               F.col("week"),
               F.col("city_id").alias("zone_id"),
               F.col("week_message"),
               F.col("week_reaction"),
               F.col("week_subscription"),
               F.col("week_user"),
               F.col("month_message"),
               F.col("month_reaction"),
               F.col("month_subscription"),
               F.col("month_user"))\
        .distinct()
    return result_df

    

def main():
    stg__geo_events_cities_geo__path = sys.argv[1]
    dm__geo_area__path = sys.argv[2]
    
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f'calculate_geo_area_dm_{current_time}') \
        .getOrCreate()
    
    events_cities_spark_df = get_stg_geo_events_cities_geo(stg__geo_events_cities_geo__path, spark)
    dm_df = add_features(events_cities_spark_df)
    dm_df.write \
        .mode('overwrite') \
        .parquet(dm__geo_area__path)
    
if __name__ == '__main__':
    main()