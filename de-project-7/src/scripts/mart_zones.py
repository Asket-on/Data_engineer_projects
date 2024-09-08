# Создать витрину в разрезе зон
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
        .appName("pr7_mart_zones") \
        .getOrCreate()
    
    df = mart(events_path, cities_path, spark)
    writer_df(df, output_path) 
    
def mart(events_path, cities_path, spark):
           
    events = spark.read.parquet(events_path)
    cities = spark.read.parquet(cities_path)
    
    events_rad = events \
    .withColumn("lat_1", F.round(F.expr("radians(lat)"), 5)) \
    .withColumn("lon_1", F.round(F.expr("radians(lon)"), 5)) \
    .drop("lat","lon") \
    .persist()

    events_rad.show(3)

    cities_rad = cities \
    .withColumn("lat_2", F.round(F.expr("radians(lat)"), 5)) \
    .withColumn("lon_2", F.round(F.expr("radians(lon)"), 5)) \
    .drop("lat","lon") \
    .persist()

    cities_rad.show(3)
    
    message = events_rad.filter(col("event_type") == "message").select(
    col('event.message_from').alias("user_id"), col('event.message_ts').alias("message_ts"), 'lat_1', 'lon_1') \
    .na.drop(subset=["message_ts", 'lat_1', 'lon_1']) 
    
    # добавляем города и дистанцию до городов
    # оставляем строки с ближайшим городом
    window = Window().partitionBy("user_id", 'message_ts').orderBy(F.asc("distance"))

    message_with_city = message.crossJoin(cities_rad) \
            .withColumn("distance", F.round(F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
                F.pow(F.sin((col('lat_2') - col('lat_1'))/F.lit(2)), 2) 
                + F.cos(col("lat_1")) * F.cos(col("lat_2")) * 
                F.pow(F.sin((col('lon_2') - col('lon_1'))/F.lit(2)), 2)
            )), 2)).orderBy('user_id', 'distance') \
    .drop("lat_1","lon_1", "lat_2","lon_2") \
    .withColumn("n", F.row_number().over(window)) \
    .orderBy('user_id', 'message_ts') \
    .filter(col("n") == 1) \
    .drop('n', 'distance') \
    .withColumnRenamed("city", "zone_id") \
    .withColumn("month",F.trunc(F.col("message_ts"), "month")) \
    .withColumn("week",F.trunc(F.col("message_ts"), "week")) \
    .persist()

    message_with_city.show(5)
    
    # таблица с message
    window_week = Window().partitionBy("week", "zone_id")
    window_month = Window().partitionBy("month", "zone_id")


    message_mart = message_with_city \
    .withColumn("week_message",F.count('user_id').over(window_week))\
    .withColumn("month_message",F.count('user_id').over(window_month))\
    .orderBy("month", "week", "zone_id") \
    .select("month", "week", "zone_id", "week_message", "month_message") \
    .distinct() \
    .persist()

    message_mart.show(5)

    # таблица с количеством регистраций
    window_first_message = Window().partitionBy("user_id").orderBy("message_ts")
    window_week = Window().partitionBy("week", "zone_id")
    window_month = Window().partitionBy("month", "zone_id")


    count_registration = message_with_city \
    .withColumn("row_number", F.row_number().over(window_first_message)) \
    .orderBy('user_id', 'message_ts') \
    .filter(col("row_number") == 1) \
    .drop('row_number') \
    .withColumn("week_user",F.count('user_id').over(window_week))\
    .withColumn("month_user",F.count('user_id').over(window_month))\
    .orderBy("month", "week", "zone_id") \
    .select("month", "week", "zone_id", "week_user", "month_user") \
    .distinct() \
    .persist()

    count_registration.show(10)

    # таблица user с привязкой city
    window1 = Window().partitionBy("user_id").orderBy(F.desc("message_ts"))

    user_with_city  = message_with_city \
    .select("user_id", "zone_id", "message_ts") \
    .withColumn("row_number", F.row_number().over(window1)) \
    .filter(col("row_number") == 1) \
    .select("user_id", "zone_id") \
    .cache()

    user_with_city.count()

    window_week = Window().partitionBy("week", "zone_id")
    window_month = Window().partitionBy("month", "zone_id")

    reaction = events_rad.filter(col("event_type") == 'reaction') \
    .select(col('event.reaction_from').alias('user_id'), 'date') \
    .withColumn("month",F.trunc(F.col("date"), "month")) \
    .withColumn("week",F.trunc(F.col("date"), "week")) \
    .join(user_with_city, ['user_id'], 'inner') \
    .withColumn("week_reaction",F.count('user_id').over(window_week))\
    .withColumn("month_reaction",F.count('user_id').over(window_month))\
    .orderBy("month", "week", "zone_id") \
    .select("month", "week", "zone_id", "week_reaction", "month_reaction") \
    .distinct() \
    .persist()

    reaction.show(5)

    subscription = events_rad.filter(col("event_type") == 'subscription') \
    .select(col('event.user').alias('user_id'), 'date') \
    .withColumn("month",F.trunc(F.col("date"), "month")) \
    .withColumn("week",F.trunc(F.col("date"), "week")) \
    .join(user_with_city, ['user_id'], 'inner') \
    .withColumn("week_subscription",F.count('user_id').over(window_week))\
    .withColumn("month_subscription",F.count('user_id').over(window_month))\
    .orderBy("month", "week", "zone_id") \
    .select("month", "week", "zone_id", "week_subscription", "month_subscription") \
    .distinct() \
    .persist()

    subscription.show(5)

    mart_zones = message_mart \
    .join(count_registration, ["month", "week", "zone_id"], how='outer') \
    .join(reaction, ["month", "week", "zone_id"], how='outer') \
    .join(subscription, ["month", "week", "zone_id"], how='outer') \
    .orderBy("month", "week", "zone_id") \
    .cache()

    mart_zones.show()
    return mart_zones

def writer_df(df, output_path):
    return df.write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()
