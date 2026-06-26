import logging

# Настроим логгер
logging.basicConfig(level=logging.ERROR)  # или logging.DEBUG для более подробного логирования
logger = logging.getLogger(__name__)

from time import sleep
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

import os

ENV = os.environ.get('ENV', 'production')

TOPIC_NAME_IN = os.environ.get('TOPIC_NAME_IN', 'restaurant_campaigns')
TOPIC_NAME_OUT = os.environ.get('TOPIC_NAME_OUT', 'campaign_triggers')

if ENV == 'local':
    postgres_connection_url_in = os.environ.get('POSTGRES_URL_IN', 'jdbc:postgresql://localhost:5432/de')
    postgres_connection_url_out = os.environ.get('POSTGRES_URL_OUT', 'jdbc:postgresql://localhost:5432/de')
    kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
    kafka_security_options = {}
    
    postgres_user_in = os.environ.get('POSTGRES_USER_IN', 'student')
    postgres_password_in = os.environ.get('POSTGRES_PASSWORD_IN', 'de-student')
    postgres_user_out = os.environ.get('POSTGRES_USER_OUT', 'jovyan')
    postgres_password_out = os.environ.get('POSTGRES_PASSWORD_OUT', 'jovyan')
else:
    postgres_connection_url_in = os.environ.get('POSTGRES_URL_IN', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
    postgres_connection_url_out = os.environ.get('POSTGRES_URL_OUT', 'jdbc:postgresql://localhost:5432/de')
    kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
    
    kafka_security_options = {
        'kafka.security.protocol': 'SASL_SSL',
        'kafka.sasl.mechanism': 'SCRAM-SHA-512',
        'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    }
    
    postgres_user_in = os.environ.get('POSTGRES_USER_IN', 'student')
    postgres_password_in = os.environ.get('POSTGRES_PASSWORD_IN', 'de-student')
    postgres_user_out = os.environ.get('POSTGRES_USER_OUT', 'jovyan')
    postgres_password_out = os.environ.get('POSTGRES_PASSWORD_OUT', 'jovyan')

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

def get_current_timestamp_utc():
    return F.unix_timestamp()


# создания Spark сессии
def create_spark_session(name) -> SparkSession:
    java_opts = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
    spark = (SparkSession
            .builder
            .config("spark.sql.session.timeZone", "UTC")
            .appName(name)
            .config("spark.jars.packages", spark_jars_packages)
            .config("spark.driver.extraJavaOptions", java_opts)
            .config("spark.executor.extraJavaOptions", java_opts)
            .getOrCreate()
            )
    return spark


# чтения Kafka-стрима с акциями от ресторанов 
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    reader = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', kafka_bootstrap_servers)
                .option("subscribe", TOPIC_NAME_IN))
    if kafka_security_options:
        reader = reader.options(**kafka_security_options)
    df = reader.load()
    
    incomming_message_schema = StructType([
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", LongType()),
            StructField("adv_campaign_datetime_end", LongType()),
            StructField("datetime_created", LongType()),
        ])

    # десериализуем из колонки value сообщения JSON
    transform_df = df \
        .withColumn("value",from_json(col("value").cast("string"), incomming_message_schema))

    # Выбираем нужные столбцы из датафрейма
    transform_df = transform_df.select("value.*")

    return transform_df

# фильтрация данных из Kafka-стрима
def filter_stream_data(df, current_timestamp_utc):
    transform_df = df.filter((col("adv_campaign_datetime_start") < current_timestamp_utc) &
    (col("adv_campaign_datetime_end") > current_timestamp_utc) )

    transform_df = transform_df.withColumn("datetime", F.to_timestamp(col("datetime_created")))

    transform_df=(transform_df.dropDuplicates(["adv_campaign_id", "datetime"])
      .withWatermark("datetime", "10 minutes").drop("datetime"))
    
    return transform_df


# вычитываем всех пользователей с подпиской на рестораны
def read_subscribers_data(spark: SparkSession) -> DataFrame:
    subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', postgres_connection_url_in) \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', postgres_user_in) \
                    .option('password', postgres_password_in) \
                    .load()

    return subscribers_restaurant_df


def join_and_transform_data(filtered_data: DataFrame, subscribers_data: DataFrame) -> DataFrame:
    joined_df = filtered_data.join(F.broadcast(subscribers_data), on="restaurant_id", how="inner") \
        .withColumn("trigger_datetime_created", F.current_timestamp())
 
    return joined_df.select(
        col("restaurant_id"),
        col("adv_campaign_id"),
        col("adv_campaign_content"),
        col("adv_campaign_owner"),
        col("adv_campaign_owner_contact"),
        col("adv_campaign_datetime_start"),
        col("adv_campaign_datetime_end"),
        col("datetime_created"),
        col("client_id"),
        col("trigger_datetime_created")
    )

# функция для записи в PostgreSQL
def write_to_postgres(df):
    # Запись DataFrame в PostgreSQL
    try:
        df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", postgres_connection_url_out) \
            .option("dbtable", "subscribers_feedback") \
            .option("user", postgres_user_out) \
            .option("password", postgres_password_out) \
            .option("driver", "org.postgresql.Driver") \
            .save()
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")   


def write_to_kafka(df):
    try:
        # сериализуем DataFrame в JSON
        df_kafka = df.select(to_json(struct("*")).alias('value'))
        writer = (df_kafka.write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                  .option("topic", TOPIC_NAME_OUT))
        if kafka_security_options:
            writer = writer.options(**kafka_security_options)
        writer.save()
        
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")


# сохранение данных в PostgreSQL и Kafka
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def save_to_postgresql_and_kafka(df, batch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    feedbacks = df.withColumn("feedback", lit(None).cast("string"))
    write_to_postgres(feedbacks)
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    write_to_kafka(df)
    # очищаем память от df
    df.unpersist()


def run_query(df):
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
    return df.writeStream \
            .trigger(processingTime='25 seconds') \
            .option("checkpointLocation", checkpoint_dir) \
            .foreachBatch(save_to_postgresql_and_kafka) \
            .start()

def main():
    spark = create_spark_session('RestaurantSubscribeStreamingService')
    restaurant_stream_df = read_kafka_stream(spark)
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    query = run_query(result_df)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(10)

    query.awaitTermination()

    spark.stop() 

if __name__ == "__main__":
    main()