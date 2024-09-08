# Построить витрину для рекомендации друзей
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

radius_geo = 6371

def main():

    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    output_path = sys.argv[3]
    today = sys.argv[4]
    
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("pr7_mart_user_rec") \
        .getOrCreate()
    
    df = mart(events_path, cities_path, spark)
    writer_df(df, output_path) 
    
def mart(events_path, cities_path, spark, today):
           
    events = spark.read.parquet(events_path)
    cities = spark.read.parquet(cities_path)
    
    #Находим пары пользователей, подписанных на один канал  
    subscription_users = (events.where("event_type='subscription'")
        .select(
            col("event.subscription_channel").alias("subscription_channel"),
            col("event.user").alias("subscription_user")
        )
        .distinct()
    )

    pairs_all = (subscription_users.alias("p1")
        .join(subscription_users.alias("p2"), 
           (col("p1.subscription_channel") == col("p2.subscription_channel")) 
              & (col("p1.subscription_user") != col("p2.subscription_user")), "inner")
        .withColumn("user_left", 
                    F.when(col("p1.subscription_user") > col("p2.subscription_user"), 
                         col("p1.subscription_user")).otherwise(col("p2.subscription_user")))
        .withColumn("user_right", 
                    F.when(col("p1.subscription_user") > col("p2.subscription_user"), 
                        col("p2.subscription_user")).otherwise(F.col("p1.subscription_user")))
        .select("user_left", "user_right")
        .distinct()
        .persist()
        )
    pairs_all.show(4)

    #исключаем тех, кто писали друг другу  
    messages = (events
        .where("event_type='message' and event.message_from is not null and event.message_to is not null") 
        .select(col("event.message_from").alias("message_from"), col("event.message_to").alias("message_to"))
        .distinct()
               )
    contacted_users = messages.union(messages.select("message_to", "message_from")).distinct()

    pairs = pairs_all.join(contacted_users, 
                       [col("user_left") == col("message_to"), 
                        col("user_right") == col("message_from")], "left")\
                .where("message_to is null") \
                .drop('message_from', 'message_to') \
                .persist()

    pairs.show(5)    

    # заменяем углы на радианы
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

    # отбираем последние по дате сообщения по отправителю

    window = Window().partitionBy('user_id').orderBy(col('message_ts').desc())
    message_lost_geo = (events_rad \
        .where('event_type = "message" and event.message_from IS NOT NULL')
        .selectExpr("event.message_from as user_id", "event.message_ts as message_ts", 
              "lat_1", "lon_1")
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop("row_number")
        .persist()
        )

    message_lost_geo.show(5)

    # добавляем координаты
    pairs_geo = pairs.join(message_lost_geo, pairs.user_left == message_lost_geo.user_id, 'inner') \
    .selectExpr('user_left', 'user_right', 'lat_1 as lat_left', 'lon_1 as lon_left') \
    .join(message_lost_geo, pairs.user_right == message_lost_geo.user_id, 'inner') \
    .selectExpr('user_left', 'user_right', 'message_ts','lat_left', 'lon_left', 'lat_1 as lat_right', 'lon_1 as lon_right') \
    .persist()

    pairs_geo.show(5)

    # отбираем строки: расстояние между ними не превышает 1 км
    pairs_geo = pairs_geo.withColumn("distance", F.round(F.lit(2) * F.lit(radius_geo) * F.asin(
            F.sqrt(
                F.pow(F.sin((col('lat_right') - col('lat_left'))/F.lit(2)), 2) 
                + F.cos(col("lat_left")) * F.cos(col("lat_right")) * 
                F.pow(F.sin((col('lon_right') - col('lon_left'))/F.lit(2)), 2)
            )), 2))\
        .orderBy(F.asc("distance")) \
        .filter(col('distance') <= 1) \
        .drop("lat_right","lon_right", 'distance') \
        .persist()
    pairs_geo.show()

    # добавляем города и дистанцию до городов
    window = Window().partitionBy("user_left", 'user_right').orderBy(F.asc("distance"))
    pairs_zone = pairs_geo.crossJoin(cities_rad) \
            .withColumn("distance", F.round(F.lit(2) * F.lit(radius_geo) * F.asin(
            F.sqrt(
                F.pow(F.sin((col('lat_2') - col('lat_left'))/F.lit(2)), 2) 
                + F.cos(col("lat_left")) * F.cos(col("lat_2")) * 
                F.pow(F.sin((col('lon_2') - col('lon_left'))/F.lit(2)), 2)
            )), 2)).orderBy('distance') \
    .withColumn("row_number", F.row_number().over(window)) \
    .filter(col("row_number") == 1) \
    .drop('row_number', "lat_2", "lon_2", "lat_left", 'lon_left') \
    .persist()

    pairs_zone.show()

    mart_user_rec = pairs_zone.withColumn("timezone", 
                F.when(
                    (col("city") == "Cranbourne") | 
                    (col("city") == "Bunbury") | 
                    (col("city") == "Ipswich") | 
                    (col("city") == "Gold Coast") |
                    (col("city") == "Townsville"), "Australia/Sydney")
                .otherwise(F.concat(F.lit("Australia/"), col("city")))) \
    .withColumn('local_time', F.from_utc_timestamp(col("message_ts"),col('timezone'))) \
    .selectExpr("user_left", "user_right", "city as zone_id",  'local_time') \
    .withColumn("processed_dttm", F.lit(today))

    mart_user_rec.show()
    
    return mart_user_rec

def writer_df(df, output_path):
    return df.write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()  
