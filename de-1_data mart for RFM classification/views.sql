CREATE OR REPLACE VIEW analysis.users_w
AS SELECT users.id,
    users.name,
    users.login
   FROM production.users;
   
CREATE OR REPLACE VIEW analysis.products_w
AS SELECT products.id,
    products.name,
    products.price
   FROM production.products;
   
CREATE OR REPLACE VIEW analysis.orderstatuses_w
AS SELECT orderstatuses.id,
    orderstatuses.key
   FROM production.orderstatuses;
   
CREATE OR REPLACE VIEW analysis.orders_w
AS SELECT orders.order_id,
    orders.order_ts,
    orders.user_id,
    orders.bonus_payment,
    orders.payment,
    orders.cost,
    orders.bonus_grant,
    orders.status
   FROM production.orders;
   
CREATE OR REPLACE VIEW analysis.orderitems_w
AS SELECT orderitems.id,
    orderitems.product_id,
    orderitems.order_id,
    orderitems.name,
    orderitems.price,
    orderitems.discount,
    orderitems.quantity
   FROM production.orderitems;