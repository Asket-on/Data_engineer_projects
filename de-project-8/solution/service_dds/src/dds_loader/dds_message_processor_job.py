from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository
from typing import Any, Dict, List


class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
    
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START==========================================")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received=============================================")
            self._logger.info('msg=================:',msg)
            # msg = {
            #     "object_id": 322519,
            #     "object_type": "order",
            #     "payload": {
            #         "id": 322519,
            #         "date": "2022-11-19 16:06:36",
            #         "cost": 300,
            #         "payment": 300,
            #         "status": "CLOSED",
            #         "restaurant": {
            #             "id": "626a81cfefa404208fe9abae",
            #             "name": "Кофейня №1"
            #         },
            #         "user": {
            #             "id": "626a81ce9a8cd1920641e296",
            #             "name": "Котова Ольга Вениаминовна"
            #         },
            #         "products": [
            #             {
            #                 "id": "6276e8cd0cf48b4cded00878",
            #                 "price": 180,
            #                 "quantity": 1,
            #                 "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
            #                 "category": "Выпечка"
            #             },
            #             {
            #                 "id": "6276e8cd0cf48b4cded0086c",
            #                 "price": 60,
            #                 "quantity": 2,
            #                 "name": "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ",
            #                 "category": "Закуски"
            #             }
            #         ]
            #     }
            # }

            payload = msg['payload']
            
            
            self._dds_repository.h_user_insert(self._dds_repository.create_h_user(payload))       
            for dict in self._dds_repository.create_h_product(payload):
                self._dds_repository.h_product_insert(dict)
            for dict in self._dds_repository.create_h_category(payload):
                self._dds_repository.h_category_insert(dict)
            self._dds_repository.h_restaurant_insert(self._dds_repository.create_h_restaurant(payload))
            self._dds_repository.h_order_insert(self._dds_repository.create_h_order(payload))

            for dict in self._dds_repository.create_l_order_product(payload):
                self._dds_repository.l_order_product_insert(dict)
            for dict in self._dds_repository.create_l_product_restaurant(payload):
                self._dds_repository.l_product_restaurant_insert(dict)
            for dict in self._dds_repository.create_l_product_category(payload):
                self._dds_repository.l_product_category_insert(dict)
            self._dds_repository.l_order_user_insert(self._dds_repository.create_l_order_user(payload))
            self._dds_repository.s_user_names_insert(self._dds_repository.create_s_user_names(payload))
            for dict in self._dds_repository.create_s_product_names(payload):
                self._dds_repository.s_product_names_insert(dict)
            self._dds_repository.s_restaurant_names_insert(self._dds_repository.create_s_restaurant_names(payload))
            self._dds_repository.s_order_cost_insert(self._dds_repository.create_s_order_cost(payload))
            self._dds_repository.s_order_status_insert(self._dds_repository.create_s_order_status(payload))

            user_id = payload["user"]["id"]
            products = payload['products']

            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "products",
                "payload": self._format_items(products, user_id)              
                }

            print(dst_msg)

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent==========================================")
            self._logger.info('dst_msg============================',dst_msg)
        self._logger.info(f"{datetime.utcnow()}: FINISH===========================================================")

    def _format_items(self, products, user_id) -> List[Dict[str, str]]:
        items = []

        for it in products:
            dst_it = {
                "id": user_id,
                "product_id": it["id"],
                "product_name": it["name"],
                "category_name": it["category"]
            }
            items.append(dst_it)

        return items