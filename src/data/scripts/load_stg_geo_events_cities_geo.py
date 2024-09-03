import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F

def input_event_paths(date, depth, events_base_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f'{events_base_path}/date={(dt - datetime.timedelta(days=x)).strftime("%Y-%m-%d")}' for x in range(depth)]


def load_stg_geo_events_cities_geo(date,
                  depth,
                  earth_radius_km,
                  source__geo_events__path,
                  ods__cities_geo__path,
                  sample_size_value):
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f'load_stg_geo_events_cities_geo_{current_time}') \
        .getOrCreate()
    events_paths = input_event_paths(date, depth, source__geo_events__path)
    events_df = spark.read \
        .option('basePath', source__geo_events__path) \
        .parquet(*events_paths) \
        .sample(sample_size_value)
    cities_df = spark \
        .read \
        .option('basePath', ods__cities_geo__path) \
        .parquet(ods__cities_geo__path) \
        .sample(sample_size_value)
    events_cities_df = events_df.crossJoin(cities_df)
    all_distance_events_cities_df = events_cities_df.withColumn('distance', F.lit(2)
                                                                * F.lit(earth_radius_km)
                                                                * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat'))
                                                                                           - F.radians(F.col(
                                                                    'city_lat'))) / F.lit(2)), 2)
                                                                                + (F.cos(F.radians(F.col('city_lat')))
                                                                                   * F.cos(F.radians(F.col('lat')))
                                                                                   * F.pow(F.sin((F.radians(F.col('lon'))
                                                                                                  - F.radians(F.col(
                                                                    'city_lng'))) / F.lit(2)), 2)))))


    min_distance_events_cities_df = all_distance_events_cities_df.withColumn('min_distance',
                                                                              F.min(F.col('distance')) \
                                                                              .over(Window.partitionBy('event',
                                                                                                       'lat',
                                                                                                       'lon') \
                                                                                    .orderBy(F.asc('distance')))) \
        .where((F.col('distance') == F.col('min_distance')) | (F.col('distance').isNull())) \
        .select(
        F.col('event'),
        F.col('lat'),
        F.col('lon'),
        F.col('city_id'),
        F.col('city_name'),
        F.col('city_lat'),
        F.col('city_lng'),
        F.col('timezone'),
        F.col('distance'),
        F.col('event_type'),
        F.col('date')
    )
    return min_distance_events_cities_df


def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    source__geo_events__path = sys.argv[3]
    ods__cities_geo__path = sys.argv[4]
    stg__geo_events_cities_geo__path = sys.argv[5]
    sample_size_value = sys.argv[6]
    
    # Constant
    earth_radius_km = 6371
    sample_size_value = float(sample_size_value)
    depth = int(depth)
    min_distance_events_cities_df = load_stg_geo_events_cities_geo(date,
                                                  depth,
                                                  earth_radius_km,
                                                  source__geo_events__path,
                                                  ods__cities_geo__path,
                                                  sample_size_value)

    min_distance_events_cities_df.write \
        .mode('overwrite') \
        .parquet(stg__geo_events_cities_geo__path)


if __name__ == '__main__':
    main()