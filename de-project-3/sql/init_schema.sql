-- Create DWH schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- 1. Create Staging Tables
CREATE TABLE IF NOT EXISTS staging.user_order_log (
    date_time TIMESTAMP,
    city_id INT,
    city_name VARCHAR(100),
    customer_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    item_id INT,
    item_name VARCHAR(200),
    quantity INT,
    payment_amount NUMERIC(14,2),
    status VARCHAR(50)
);

-- 2. Create Dimension Tables
CREATE TABLE IF NOT EXISTS mart.d_city (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mart.d_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city_id INT
);

CREATE TABLE IF NOT EXISTS mart.d_item (
    item_id INT PRIMARY KEY,
    item_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS mart.d_calendar (
    date_id SERIAL PRIMARY KEY,
    date_actual DATE UNIQUE NOT NULL,
    week_of_year INT NOT NULL,
    first_day_of_week DATE NOT NULL,
    last_day_of_week DATE NOT NULL
);

-- 3. Create Fact & Mart Tables
CREATE TABLE IF NOT EXISTS mart.f_sales (
    id SERIAL PRIMARY KEY,
    date_id INT REFERENCES mart.d_calendar(date_id),
    item_id INT REFERENCES mart.d_item(item_id),
    customer_id INT REFERENCES mart.d_customer(customer_id),
    city_id INT REFERENCES mart.d_city(city_id),
    quantity INT NOT NULL,
    payment_amount NUMERIC(14,2) NOT NULL,
    status VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count INT NOT NULL,
    returning_customers_count INT NOT NULL,
    refunded_customer_count INT NOT NULL,
    period_name VARCHAR(100) NOT NULL,
    period_id INT NOT NULL,
    item_id INT REFERENCES mart.d_item(item_id),
    new_customers_revenue NUMERIC(14,2) NOT NULL,
    returning_customers_revenue NUMERIC(14,2) NOT NULL,
    customers_refunded INT NOT NULL,
    PRIMARY KEY (period_id, item_id)
);

-- 4. Pre-populate d_calendar dimension using generate_series
INSERT INTO mart.d_calendar (date_actual, week_of_year, first_day_of_week, last_day_of_week)
SELECT
    datum AS date_actual,
    EXTRACT(week FROM datum) AS week_of_year,
    (datum - (EXTRACT(isodow FROM datum) - 1)::INT * INTERVAL '1 day')::DATE AS first_day_of_week,
    (datum + (7 - EXTRACT(isodow FROM datum))::INT * INTERVAL '1 day')::DATE AS last_day_of_week
FROM generate_series('2025-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) AS datum
ON CONFLICT (date_actual) DO NOTHING;
