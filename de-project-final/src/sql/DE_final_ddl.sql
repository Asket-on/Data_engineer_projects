DROP TABLE STV202311131__STAGING.transactions CASCADE;
DROP TABLE STV202311131__STAGING.currencies CASCADE;
DROP TABLE STV202311131__DWH.global_metrics CASCADE;

-- Create the main table
CREATE TABLE STV202311131__STAGING.transactions (
    operation_id VARCHAR(60) NULL,
    account_number_from INT NULL,
    account_number_to INT NULL,
    currency_code INT NULL,
    country VARCHAR(30) NULL,
    status VARCHAR(30) NULL,
    transaction_type VARCHAR(30) NULL,
    amount INT NULL,
    transaction_dt TIMESTAMP NULL,
    CONSTRAINT unique_transaction UNIQUE (operation_id) ENABLED
);

-- Create a superprojection with sorting by transaction_dt
CREATE PROJECTION STV202311131__STAGING.transactions_super AS
    SELECT *
    FROM STV202311131__STAGING.transactions
    ORDER BY transaction_dt;

-- Create a segmented projection using hash function on transaction_dt and operation_id
CREATE PROJECTION STV202311131__STAGING.transactions_segmented AS
    SELECT *
    FROM STV202311131__STAGING.transactions
    SEGMENTED BY HASH(transaction_dt, operation_id)
    ALL NODES KSAFE 1;
   
   -- Создаем основную таблицу
CREATE TABLE STV202311131__STAGING.currencies (
    date_update TIMESTAMP NULL,
    currency_code INT NULL,
    currency_code_with INT NULL,
    currency_with_div NUMERIC(5, 3) NULL,
    CONSTRAINT unique_currency UNIQUE (date_update, currency_code, currency_code_with) ENABLED
);

-- Создаем суперпроекцию с сортировкой по date_update
CREATE PROJECTION STV202311131__STAGING.currencies_super AS
    SELECT *
    FROM STV202311131__STAGING.currencies
    ORDER BY date_update;

-- Создаем сегментированную проекцию с использованием хеш-функции от date_update и currency_code
CREATE PROJECTION STV202311131__STAGING.currencies_segmented AS
    SELECT *
    FROM STV202311131__STAGING.currencies
    SEGMENTED BY HASH(date_update, currency_code)
    ALL NODES KSAFE 1;
   
CREATE TABLE STV202311131__DWH.global_metrics (
    date_update DATE  NOT NULL,
    currency_from INT  NOT NULL,
    amount_total DECIMAL(18, 2)  NOT NULL,
    cnt_transactions INT  NOT NULL,
    avg_transactions_per_account DECIMAL(18, 2)  NOT NULL,
    cnt_accounts_make_transactions INT  NOT NULL,
    CONSTRAINT pk PRIMARY KEY (date_update, currency_from) ENABLED
);


