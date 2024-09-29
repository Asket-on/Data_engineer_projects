import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository

app = Flask(__name__)

@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    config = AppConfig()
    print(f"Kafka Host=================================: {config.kafka_host}")
    dds_repository = DdsRepository(config.pg_warehouse_db())
    kafka_producer = config.kafka_producer()
    kafka_consumer = config.kafka_consumer()
    print('kafka_consumer============================',kafka_consumer)
    proc = DdsMessageProcessor(
        kafka_consumer, kafka_producer, dds_repository, 30, app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', port=5004, use_reloader=False)
