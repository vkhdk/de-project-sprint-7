import os
import sys
import datetime
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytz
#iteritems is removed from pandas 2.0
#assign DataFrame.items to DataFrame.iteritems
pd.DataFrame.iteritems = pd.DataFrame.items

def main():
    print('Loading city geo data')
    local_path = sys.argv[1]
    hdfs_path = sys.argv[2]
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    spark = SparkSession.builder \
                    .master('yarn') \
                    .appName(f'load_city_geo_{current_time}')\
                    .getOrCreate()
    #get local data
    cities_geo_local_df = pd.read_csv(local_path, sep = ';')
    cities_geo_local_df['city'] = cities_geo_local_df['city'].str.replace(' ', '_')
    cities_geo_local_df['timezone'] = cities_geo_local_df['city'].apply(lambda city: 
                                      next((tz for tz in pytz.all_timezones if city.lower() in tz.lower()), 'Australia/Sydney')
                                                                       )
    #transforme data
    cities_geo_local_spark_df = spark.createDataFrame(cities_geo_local_df)
    cities_geo_local_spark_df = cities_geo_local_spark_df.withColumnRenamed('id', 'city_id') \
                                                         .withColumnRenamed('city', 'city_name') \
                                                         .withColumnRenamed('lat', 'city_lat') \
                                                         .withColumnRenamed('lng', 'city_lng') \
                                                         .withColumn('city_lat', F.regexp_replace('city_lat', ',', '.').cast('double')) \
                                                         .withColumn('city_lng', F.regexp_replace('city_lng', ',', '.').cast('double'))
    #save local data to hdfs 
    cities_geo_local_spark_df.write\
                             .mode('overwrite')\
                             .parquet(hdfs_path)
    return 'City geo data loaded'
    
if __name__ == '__main__':
    main()   