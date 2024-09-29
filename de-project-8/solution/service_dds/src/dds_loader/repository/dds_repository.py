import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel

class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str      

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str 


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: str
    load_dt: datetime
    load_src: str 

class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Product_Сategory(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str 
    hk_user_names_hashdiff: uuid.UUID

class S_Product_Names (BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str 
    hk_product_names_hashdiff: uuid.UUID

class S_Restaurant_Names (BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_name: str
    load_dt: datetime
    load_src: str 
    hk_restaurant_names_hashdiff: uuid.UUID

class S_Order_Cost (BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str 
    hk_order_cost_hashdiff: uuid.UUID   

class S_Order_Status (BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str 
    hk_order_status_hashdiff: uuid.UUID

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self.source = 'stg-service-orders'
        self.ns_uuid = uuid.UUID('d4dc5ac0-49c6-49c9-b5f4-5fdc183e9c7a')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.ns_uuid, name=str(obj))

    def h_user_insert (self, obj: H_User) -> None:                           
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                       INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'user_id': obj.user_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def create_h_user(self, data: str) -> H_User:
        user_id = data["user"]["id"]
        return H_User(h_user_pk=self._uuid(user_id), user_id=user_id, load_dt=datetime.utcnow(), load_src=self.source)


    def create_h_product(self, data: str) -> List[H_Product]:
        products = []
        for prod in data['products']:
            product_id = prod['id']
            products.append(H_Product(h_product_pk=self._uuid(product_id),
                    product_id=product_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source))
        return products


    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(h_product_pk, product_id, load_dt,load_src)
                        VALUES(%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(h_category_pk, category_name, load_dt, load_src)
                        VALUES(%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def create_h_category(self, data: str) -> List[H_Category]:
        category = []
        for prod in data['products']:
            category_name = prod['category']
            category.append(H_Category(h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source))
        return category
    
    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt, load_src)
                        VALUES(%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def create_h_restaurant(self, data: str) -> H_Restaurant:
        restaurant_id = data["restaurant"]["id"]
        return H_Restaurant(h_restaurant_pk=self._uuid(restaurant_id), restaurant_id=restaurant_id, load_dt=datetime.utcnow(), load_src=self.source)
    

    def h_order_insert(self, obj: H_Order) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.h_order(h_order_pk, order_id, order_dt, load_dt, load_src)
                            VALUES(%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (h_order_pk) DO NOTHING;
                        """,
                        {
                            'h_order_pk': obj.h_order_pk,
                            'order_id': obj.order_id,
                            'order_dt': obj.order_dt,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def create_h_order(self, data: str) -> H_Order:
        order_id = str(data["id"])
        order_dt = str(data["date"])
        return H_Order(h_order_pk=self._uuid(order_id), order_id=order_id, order_dt=order_dt, load_dt=datetime.utcnow(), load_src=self.source)

    def l_order_product_insert(self, obj: L_Order_Product) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                            VALUES(%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_order_product_pk) DO NOTHING;
                        """,
                        {
                            'hk_order_product_pk': obj.hk_order_product_pk,
                            'h_order_pk': obj.h_order_pk,
                            'h_product_pk': obj.h_product_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def create_l_order_product(self, data: str) -> List[L_Order_Product]:
        order_id = str(data["id"])
        order_product = []
        for prod in data['products']:
            product_id = prod['id']
            order_product.append(L_Order_Product(hk_order_product_pk=self._uuid(order_id+product_id),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(product_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source))
        return order_product
    

    def l_product_restaurant_insert(self, obj: L_Product_Restaurant) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_restaurant(hk_product_restaurant_pk, 
                            h_product_pk, h_restaurant_pk, load_dt, load_src)
                            VALUES(%(hk_product_restaurant_pk)s, %(h_product_pk)s, 
                            %(h_restaurant_pk)s, %(load_dt)s,  %(load_src)s)
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                            'h_product_pk': obj.h_product_pk,
                            'h_restaurant_pk': obj.h_restaurant_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def create_l_product_restaurant(self, data: str) -> List[L_Product_Restaurant]:
        restaurant_id = str(data["restaurant"]["id"])
        product_restaurant = []
        for prod in data['products']:
            product_id = prod['id']
            product_restaurant.append(L_Product_Restaurant(hk_product_restaurant_pk=self._uuid(restaurant_id+product_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    h_product_pk=self._uuid(product_id),    
                    load_dt=datetime.utcnow(),
                    load_src=self.source))
        return product_restaurant
    

    def l_product_category_insert(self, obj: L_Product_Сategory) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_category(hk_product_category_pk, 
                            h_product_pk, h_category_pk, load_dt, load_src)
                            VALUES(%(hk_product_category_pk)s, %(h_product_pk)s, 
                            %(h_category_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_product_category_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_category_pk': obj.hk_product_category_pk,
                            'h_product_pk': obj.h_product_pk,
                            'h_category_pk': obj.h_category_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def create_l_product_category(self, data: str) -> List[L_Product_Сategory]:
        product_category = []
        for prod in data['products']:
            prod_id = prod['id']
            category_name = prod['category']
            product_category.append(
                L_Product_Сategory(
                    hk_product_category_pk=self._uuid(category_name+prod_id),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(category_name),    
                    load_dt=datetime.utcnow(),
                    load_src=self.source
                )
            )
        return product_category


    def l_order_user_insert(self, obj: L_Order_User) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                            VALUES(%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_order_user_pk) DO NOTHING;
                        """,
                        {
                            'hk_order_user_pk': obj.hk_order_user_pk,
                            'h_order_pk': obj.h_order_pk,
                            'h_user_pk': obj.h_user_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def create_l_order_user(self, data: str) -> L_Order_User:
        order_id = str(data["id"])
        user_id = str(data["user"]["id"])
        return L_Order_User(hk_order_user_pk=self._uuid(order_id+user_id),
                h_order_pk=self._uuid(order_id),
                h_user_pk=self._uuid(user_id),    
                load_dt=datetime.utcnow(),
                load_src=self.source)
    

    def s_user_names_insert (self, obj: S_User_Names) -> None: 
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                       INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                        VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
                            ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_user_pk': obj.h_user_pk,
                            'username': obj.username,
                            'userlogin': obj.userlogin,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                        }
                    )
                
    def create_s_user_names(self, data: str) -> S_User_Names:
        user_id = data["user"]["id"]
        user_name = data["user"]["name"]
        return S_User_Names(h_user_pk=self._uuid(user_id), 
                            username = user_name, 
                            userlogin = user_name, 
                            load_dt = datetime.utcnow(), 
                            load_src = self.source,
                            hk_user_names_hashdiff = self._uuid(user_id+user_name+str(datetime.utcnow())+self.source))
    

    def s_product_names_insert(self, obj: S_Product_Names) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                            VALUES(%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
                            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_product_pk': obj.h_product_pk,
                            'name': obj.name,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                        }
                    )

    def create_s_product_names(self, data: str) -> List[S_Product_Names]:
        product_names = []
        for prod in data['products']:
            product_id = prod['id']
            name = prod['name']

            product_names.append(S_Product_Names(h_product_pk=self._uuid(product_id),
                    name=name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source,
                    hk_product_names_hashdiff=self._uuid(product_id+name+str(datetime.utcnow())+self.source)        
                )
            )
        return product_names
    
    def s_restaurant_names_insert(self, obj: S_Restaurant_Names) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_restaurant_names(h_restaurant_pk,  name, load_dt, load_src, hk_restaurant_names_hashdiff)
                            VALUES(%(h_restaurant_pk)s, %(restaurant_name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
                            ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_restaurant_pk': obj.h_restaurant_pk,
                            'restaurant_name': obj.restaurant_name,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                        }
                    )

    def create_s_restaurant_names(self, data: str) -> List[S_Restaurant_Names]:
        restaurant_id = data['restaurant']['id']
        restaurant_name = data['restaurant']['name']

        return S_Restaurant_Names(h_restaurant_pk=self._uuid(restaurant_id),
                restaurant_name=restaurant_name,
                load_dt=datetime.utcnow(),
                load_src=self.source,
                hk_restaurant_names_hashdiff=self._uuid(restaurant_id+restaurant_name+str(datetime.utcnow())+self.source))
    
    def s_order_cost_insert(self, obj: S_Order_Cost) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_order_cost(h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                            VALUES(%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
                            ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_order_pk': obj.h_order_pk,
                            'cost': obj.cost,
                            'payment': obj.payment,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                        }
                    )

    def create_s_order_cost(self, data: str) -> List[S_Order_Cost]:
        order_id = data['id']
        payment = data['payment']
        cost = data['cost']

        return S_Order_Cost(h_order_pk=self._uuid(order_id),
                cost=cost,
                payment=payment,
                load_dt=datetime.utcnow(),
                load_src=self.source,
                hk_order_cost_hashdiff=self._uuid(str(order_id)+str(cost)+str(payment)+str(datetime.utcnow())+self.source)        
            )
    

    def s_order_status_insert(self, obj: S_Order_Status) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_order_status(h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                            VALUES(%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
                            ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_order_pk': obj.h_order_pk,
                            'status': obj.status,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                        }
                    )

    def create_s_order_status(self, data: str) -> List[S_Order_Status]:
        order_id = data['id']
        status = data['status']

        return S_Order_Status(h_order_pk=self._uuid(order_id),
                status=status,
                load_dt=datetime.utcnow(),
                load_src=self.source,
                hk_order_status_hashdiff=self._uuid(str(order_id)+status+str(datetime.utcnow())+self.source)        
            )
