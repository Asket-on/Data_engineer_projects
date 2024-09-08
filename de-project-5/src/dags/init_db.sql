--DROP TABLE IF EXISTS stg.deliverysystem_couriers;
--DROP TABLE IF EXISTS stg.deliverysystem_deliveries;
--DROP TABLE IF EXISTS dds.fct_api_sales;
--DROP TABLE IF EXISTS dds.dm_api_delivery_details;
--DROP TABLE IF EXISTS dds.dm_api_couriers;
--DROP TABLE IF EXISTS dds.dm_api_orders;
--DROP TABLE IF EXISTS cdm.dm_courier_ledger;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
object_id varchar NOT NULL UNIQUE,
object_value text NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
object_id varchar NOT NULL UNIQUE,
object_value text NOT NULL
);
                       
CREATE TABLE if not exists dds.dm_api_orders (
id serial PRIMARY KEY ,
order_id  varchar NOT null unique,
order_ts timestamp NOT NULL);

CREATE TABLE if not exists dds.dm_api_couriers (
id serial PRIMARY KEY ,
courier_id varchar NOT null unique,
courier_name VARCHAR NOT NULL);

CREATE TABLE if not exists dds.dm_api_delivery_details (
id serial PRIMARY KEY ,
delivery_id varchar NOT null unique,
courier_id varchar NOT NULL,
delivery_ts timestamp NOT NULL,
rate smallint NOT null,
CONSTRAINT dm_api_delivery_details_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_api_couriers(courier_id)
);

CREATE TABLE if not exists dds.fct_api_sales (
id serial PRIMARY KEY ,
order_id  varchar NOT null unique,
delivery_id  varchar NOT NULL,
order_sum numeric (12,2) NOT NULL,
tip_sum  numeric (12,2) NOT null,
constraint fct_api_sales_delivery_id_fkey 
FOREIGN KEY (delivery_id) references dds.dm_api_delivery_details(delivery_id),
constraint fct_api_sales_order_id_fkey FOREIGN KEY (order_id) 
references dds.dm_api_orders(order_id)
);


CREATE TABLE if not exists cdm.dm_courier_ledger (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year SMALLINT NOT NULL CHECK(settlement_year>= 2022 AND settlement_year < 2500),
    settlement_month SMALLINT NOT NULL CHECK(settlement_month>= 1 AND settlement_month <= 12),
    orders_count int NOT NULL CHECK(orders_count >= 0),
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg numeric(14, 5) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum  numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
    UNIQUE(courier_id, settlement_year, settlement_month)
);	