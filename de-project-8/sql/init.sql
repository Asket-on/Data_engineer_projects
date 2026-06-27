-- Create schemas
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;

-- STG layer tables
CREATE TABLE IF NOT EXISTS stg.order_events (
    id SERIAL,
    object_id INT PRIMARY KEY,
    object_type VARCHAR(50) NOT NULL,
    sent_dttm TIMESTAMP NOT NULL,
    payload JSONB NOT NULL
);

-- DDS layer Hubs
CREATE TABLE IF NOT EXISTS dds.h_user (
    h_user_pk UUID PRIMARY KEY,
    user_id VARCHAR(100) UNIQUE NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_product (
    h_product_pk UUID PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_category (
    h_category_pk UUID PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_restaurant (
    h_restaurant_pk UUID PRIMARY KEY,
    restaurant_id VARCHAR(100) UNIQUE NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_order (
    h_order_pk UUID PRIMARY KEY,
    order_id VARCHAR(100) UNIQUE NOT NULL,
    order_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

-- DDS layer Links
CREATE TABLE IF NOT EXISTS dds.l_order_product (
    hk_order_product_pk UUID PRIMARY KEY,
    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
    hk_product_restaurant_pk UUID PRIMARY KEY,
    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
    h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.l_product_category (
    hk_product_category_pk UUID PRIMARY KEY,
    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
    h_category_pk UUID NOT NULL REFERENCES dds.h_category(h_category_pk),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.l_order_user (
    hk_order_user_pk UUID PRIMARY KEY,
    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
    h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL
);

-- DDS layer Satellites
CREATE TABLE IF NOT EXISTS dds.s_user_names (
    h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
    username VARCHAR(100) NOT NULL,
    userlogin VARCHAR(100) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL,
    hk_user_names_hashdiff UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dds.s_product_names (
    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
    name VARCHAR(100) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL,
    hk_product_names_hashdiff UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
    h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
    name VARCHAR(100) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL,
    hk_restaurant_names_hashdiff UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dds.s_order_cost (
    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
    cost NUMERIC(14, 2) NOT NULL,
    payment NUMERIC(14, 2) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL,
    hk_order_cost_hashdiff UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dds.s_order_status (
    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
    status VARCHAR(50) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(100) NOT NULL,
    hk_order_status_hashdiff UUID PRIMARY KEY
);

-- CDM layer tables
CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    order_cnt INT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, product_id)
);

CREATE TABLE IF NOT EXISTS cdm.user_category_counters (
    user_id UUID NOT NULL,
    category_id UUID NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    order_cnt INT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, category_id)
);
