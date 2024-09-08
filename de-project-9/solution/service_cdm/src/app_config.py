import os

from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect


class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST'))
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')))
        self.kafka_consumer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME'))
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP'))
        self.kafka_consumer_topic = str(os.getenv('KAFKA_DDS_SERVICE_ORDERS_TOPIC'))
        # self.kafka_producer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME'))
        # self.kafka_producer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
        # self.kafka_producer_topic = str(os.getenv('KAFKA_DESTINATION_TOPIC'))

        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST'))
        self.pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT')))
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME'))
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER'))
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD'))

        # self.kafka_host = "rc1a-imk8jqg14og1moha.mdb.yandexcloud.net"
        # self.kafka_port = 9091
        # self.kafka_consumer_username = "producer_consumer"
        # self.kafka_consumer_password = "pass1234"
        # self.kafka_consumer_group = "aske-kafka397"
        # self.kafka_consumer_topic = "dds-service-orders"

        # self.pg_warehouse_host = "rc1b-0oe13fcaemm3sxfy.mdb.yandexcloud.net"
        # self.pg_warehouse_port = 6432
        # self.pg_warehouse_dbname = "sprint9dwh"
        # self.pg_warehouse_user = "db_user"
        # self.pg_warehouse_password = "pass1234"        


    def kafka_consumer(self):
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
