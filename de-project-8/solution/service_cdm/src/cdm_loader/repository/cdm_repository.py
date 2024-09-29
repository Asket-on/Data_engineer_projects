import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect

from pydantic import BaseModel

class CdmCategoryProduct(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str 
    product_id: uuid.UUID
    product_name: str

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

        self.ns_uuid = uuid.UUID('d4dc5ac0-49c6-49c9-b5f4-5fdc183e9c7a')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.ns_uuid, name=str(obj))

    def category_product_create(self, data: str) -> List[CdmCategoryProduct]:
        category_product = []
        for it in data:
            category_product.append(CdmCategoryProduct(
                    user_id=self._uuid(it['id']),
                    product_id=self._uuid(it['product_id']),
                    product_name=it['product_name'],
                    category_id=self._uuid(it['category_name']),
                    category_name=it['category_name']        
                )
            )
        return category_product
    

    def cdm_product_insert(self, obj: CdmCategoryProduct) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                            VALUES(%(user_id)s, %(product_id)s, %(product_name)s, 1)
                            ON CONFLICT (user_id, product_id) DO UPDATE
                            SET order_cnt = user_product_counters.order_cnt + 1;
                        """,
                        {
                            'user_id': obj.user_id,
                            'product_id': obj.product_id,
                            'product_name': obj.product_name,
                        }
                    )

    def cdm_category_insert(self, obj: CdmCategoryProduct) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                            VALUES(%(user_id)s, %(category_id)s, %(category_name)s, 1)
                            ON CONFLICT (user_id, category_id) DO UPDATE
                            SET order_cnt = cdm.user_category_counters.order_cnt + 1;
                        """,
                        {
                            'user_id': obj.user_id,
                            'category_id': obj.category_id,
                            'category_name': obj.category_name,
                        }
                    )