# Создать витрину в разрезе пользователей
import sys
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def main():

    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    output_path = sys.argv[3]
    
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("pr7_mart_user_travel_info") \
        .getOrCreate()
    
    df = mart(events_path, cities_path, spark)
    writer_df(df, output_path) 
    
def mart(events_path, cities_path, spark):
    
    events = spark.read.parquet(events_path)
    cities = spark.read.parquet(cities_path)


    message = events.filter(col("event_type") == "message").select(
        col('event.message_from').alias("user_id"), col('event.message_ts').alias("message_ts"), 'lat', 'lon') \
        .na.drop(subset=["message_ts", "lat", "lon"]) \
        .withColumn("lat_1", F.round(F.expr("radians(lat)"), 5)) \
        .withColumn("lon_1", F.round(F.expr("radians(lon)"), 5)) \
        .drop("lat","lon") \
        .persist()

    message.show(3)
    message.count()

    cities_rad = cities \
    .withColumn("lat_2", F.round(F.expr("radians(lat)"), 5)) \
    .withColumn("lon_2", F.round(F.expr("radians(lon)"), 5)) \
    .drop("lat","lon") \
    .persist()

    cities_rad.show(3)

    # добавляем города и дистанцию до городов
    message_cross = message.crossJoin(cities_rad) \
            .withColumn("distance", F.round(F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
                F.pow(F.sin((col('lat_2') - col('lat_1'))/F.lit(2)), 2) 
                + F.cos(col("lat_1")) * F.cos(col("lat_2")) * 
                F.pow(F.sin((col('lon_2') - col('lon_1'))/F.lit(2)), 2)
            )), 2)).orderBy('user_id', 'distance') \
    .drop("lat_1","lon_1", "lat_2","lon_2") \
    .persist()
    message_cross.show(4)

    # оставляем строки с ближайшим городом
    window = Window().partitionBy("user_id", 'message_ts').orderBy(F.asc("distance"))

    message_city = message_cross \
    .withColumn("n", F.row_number().over(window)) \
    .orderBy('user_id', 'message_ts') \
    .filter(col("n") == 1) \
    .drop('n') \
    .persist()

    message_city.show(5)

    window1 = Window().partitionBy("user_id").orderBy(F.desc("message_ts"))
    window2 = Window().partitionBy("user_id").orderBy("message_ts")
    window3 = Window().partitionBy("user_id")


    mart_user_travel_info = message_city \
    .select("user_id", "city", "message_ts") \
    .withColumn("row_number", F.row_number().over(window1)) \
    .withColumn("prev_city", F.lag("city").over(window2)) \
    .withColumn("prev_message_ts", F.lag("message_ts").over(window2)) \
    .withColumn("act_city", F.when(col("row_number") == 1, col("city")))\
    .withColumn("lost_time", F.when(col("row_number") == 1, col("message_ts")))\
    .withColumn("city_change_indicator", F.when(col("city") != col("prev_city"),1).otherwise(0))\
    .withColumn("travel_count", F.sum("city_change_indicator").over(window2)) \
    .groupBy("user_id", "city", "travel_count")\
    .agg(F.expr("min(message_ts) as min_dt"),
       F.expr("max(message_ts) as max_dt"),
       F.expr("max(act_city) as act_city"),
       F.expr("max(lost_time) as TIME_UTC"))\
    .withColumn('travel_array', F.collect_list('city').over(window3)) \
    .withColumn("date_diff_days", F.datediff("max_dt", "min_dt")) \
    .withColumn("home_city", F.when(col("date_diff_days") >= 27,  col("city"))) \
    .persist() \
    .filter(col("act_city").isNotNull()) \
    .withColumn("timezone", 
                F.when(
                    (col("act_city") == "Cranbourne") | 
                    (col("act_city") == "Bunbury") | 
                    (col("act_city") == "Townsville"), "Australia/Sydney")
                .otherwise(F.concat(F.lit("Australia/"), col("act_city")))) \
    .withColumn('local_time', F.from_utc_timestamp(col("TIME_UTC"),col('timezone'))) \
    .select("user_id", "act_city", "home_city", "travel_count", 'travel_array', 'local_time')

    mart_user_travel_info.show(5)
    return mart_user_travel_info

def writer_df(df, output_path):
    return df.write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()
