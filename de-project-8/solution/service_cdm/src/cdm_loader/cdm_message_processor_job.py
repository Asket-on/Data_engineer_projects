from datetime import datetime
from logging import Logger
from cdm_loader.repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer



class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 ) -> None:
        
        self._consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 20


    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

        # msg = {'object_id': 322519, 'object_type': 'order', 
        #        'payload': [{'id': '626a81ce9a8cd1920641e296', 
        #                     'product_id': '626a81ce9a8cd1920641e296', 
        #                     'product_name': 'РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ', 
        #                     'category_name': 'Выпечка'}, 
        #                     {'id': '626a81ce9a8cd1920641e296', 
        #                      'product_id': '626a81ce9a8cd1920641e296', 
        #                      'product_name': 'ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ', 
        #                      'category_name': 'Закуски'}]}
        
            for dict in self._cdm_repository.category_product_create(msg['payload']):
                self._cdm_repository.cdm_product_insert(dict)
                self._cdm_repository.cdm_category_insert(dict)      


        self._logger.info(f"{datetime.utcnow()}: FINISH")
