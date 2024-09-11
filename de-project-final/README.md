# Final Project

### Description

This project presents the data work of a fintech startup that offers international banking services through an application: users can safely transfer money to different countries.

Analytics team made a request to collect data on user transaction activity and set up an update to the table with exchange rates.

The goal is to understand what the turnover dynamics of the entire company looks like and what leads to its changes.

### Workflow schema

![](/src/img/Workflow_schema_DE_final_project.png)

### Unpacking infrastructure using following pipeline

#### 1. Local docker

```bash
docker-compose up -d
```

- Metabase connection `http://localhost:8998/`, account — `<...>`; password — `<...>`
- Airflow connection `http://localhost:8280/airflow/`, account — `AirflowAdmin`; password — `<...>`
- PostgreSQL connection: account — `<...>`; password — `<...>`.

#### 2. PostgreSQL

data structure in `transactions` table: 

- `operation_id` — transaction id;
- `account_number_from` — internal accounting number of the transaction account FROM WHOM;
- `account_number_to` — internal accounting account number of the transaction TO WHOM;
- `currency_code` — three-digit code of the currency of the country from which the transaction originates;
- `country` — transaction source country;
- `status` — transaction status:
	- **queued** (“transaction in queue for processing by the service”),
	- **in_progress** (“transaction in progress”),
	- **blocked** (“transaction is blocked by the service”),
	- **done** (“transaction completed successfully”),
	- **chargeback** (“the user has made a chargeback for the transaction”).
- `transaction_type` — transaction type in internal accounting:
	- **authorisation** (“authorization transaction confirming the existence of a user account”),
	- **sbp_incoming** (“incoming transfer via the fast payment system”),
	- **sbp_outgoing** (“outgoing transfer using the fast payment system”),
	- **transfer_incoming** (“incoming account transfer”),
	- **transfer_outgoing** (“outgoing account transfer”),
	- **c2b_partner_incoming** (“transfer from a legal entity”),
	- **c2b_partner_outgoing** (“transfer to a legal entity”).
- `amount` — integer transaction amount in the minimum unit of the country’s currency (kopeck, cent, kurush);
- `transaction_dt` — date and time of transaction execution up to milliseconds.

data structure in `currencies` table: 

- `date_update` — date of update of the exchange rate;
- `currency_code` — three-digit transaction currency code;
- `currency_code_with` — the ratio of another currency to the currency of the three-digit code;
- `currency_code_div` - the value of the ratio of a unit of one currency to a unit of the transaction currency.

#### 3. AirFlow

1. setup the connection to Postgres, name = postgres_conn
```
postgres_conn = {
    "host": "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
    "port": 6432,
    "user": "student",
    "password": "<...>",
    "database": "db1",
	{
		"sslmode": "verify-ca",  
		"sslcert": "/lessons/cert/CA.pem"
	}
}
```

2. setup the connection to Vertica, name = vertica_conn

```
vertica_conn = {
    "host": "vertica.tgcloudenv.ru",
    "port": 5433,
    "user": "<...>",
    "password": "<...>",
    "database": "dwh"
}
```

3. Run DAGs (`1_DAG_postg_to_vert`, `2_DAG_cdm`) one by one which creates STG, DDS and CDM layers.

4. Run backfill in conteiner with airflow with desired historical period (october 2022)


```bash
# STG LAYER
docker exec -it $(docker ps -q) bash
airflow dags backfill 3_STG_vertica_load --start-date 2022-10-01 --end-date 2022-10-31
```

```bash
# DDS LAYER
docker exec -it $(docker ps -q) bash
airflow dags backfill 4_DDS_vertica_load --start-date 2022-10-01 --end-date 2022-10-02
```

```bash
# CDM LAYER
docker exec -it $(docker ps -q) bash
airflow dags backfill 5_CDM_vertica_load --start-date 2022-10-01 --end-date 2022-10-01
```

Data structure in `global_metrics` table in CDM layer: 

- `date_update` — calculation date,
- `currency_from` — transaction currency code;
- `amount_total` — total amount of transactions by currency in dollars;
- `cnt_transactions` — total volume of transactions by currency;
- `avg_transactions_per_account` — average volume of transactions per account;
- `cnt_accounts_make_transactions` — the number of unique accounts with completed transactions by currency.


5. When the data uploaded to CDM layer it become possible to visualize the final data mart.

#### 4. Metabase

![](/src/img/Dash_sp10_de_2024-03-01_11-48-23.png)


### Install Virtual ENV and dependencies

Creating a virtual environment

```python3 -m venv venv```

Activation of the virtual environment:

```source venv/bin/activate```

Update pip to latest version:

```pip install --upgrade pip```

Install Dependencies:

```pip install -r requirements.txt```

### Repository structure
The files in the repository will be used for review and feedback on the project. Therefore, try to publish your solution according to the established structure: this will make it easier to relate tasks to solutions.

Inside `src` there are folders:
- `/src/dags` - place the DAG code in this folder, which supplies data from the source to the storage. Name the DAG `1_DAG_postg_to_vert.py`. Also place the DAG here that updates the data marts. Name the DAG `2_DAG_cdm.py`.
- `/src/sql` - here insert the SQL query for forming tables in the `STAGING` and `DWH` layers, as well as the data preparation script for the final showcase.
- `/src/img` - here place a screenshot of the dashboard implemented above the showcase.
