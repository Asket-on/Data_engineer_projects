# Airflow API Ingestion & Customer Retention Mart (`de-project-3`)

This project implements an automated, incremental ETL pipeline inside Apache Airflow. It extracts daily customer transaction logs from an external HTTP API, loads them into a PostgreSQL staging layer, normalizes dimensions and facts, and compiles a weekly **Customer Retention Mart** (`mart.f_customer_retention`).

---

## Business Context

Understanding customer loyalty is key to retail analytics. This pipeline tracks:
1. **New Customers**: Customers who made their first purchase in a given week.
2. **Returning Customers**: Customers who made multiple purchases in distinct weeks.
3. **Refunded Customers**: Customers who cancelled/refunded their orders in a given week.
4. **Revenue**: Breakdowns of new vs. returning customer spending.

The data is compiled weekly at the grain of `(period_id, item_id)`.

---

## System Flow & Architecture

The pipeline is coordinated entirely by Apache Airflow:

```mermaid
flowchart TD
    %% Airflow DAG Sequence
    subgraph airflow_dag ["Airflow DAG: customer_retention"]
        init["1. init_schema (PostgresOperator)"]
        gen["2. generate_report (PythonOperator)"]
        poll["3. get_report (PythonOperator)"]
        inc["4. get_increment (PythonOperator)"]
        load["5. upload_user_order_inc (PythonOperator)"]
        
        %% Dimension Updates
        dim_item["6a. update_d_item (PostgresOperator)"]
        dim_cust["6b. update_d_customer (PostgresOperator)"]
        dim_city["6c. update_d_city (PostgresOperator)"]
        
        %% Facts & Marts
        fact["7. update_f_sales (PostgresOperator)"]
        mart["8. update_f_customer_retention (PostgresOperator)"]
    end

    %% External Infrastructure Mock
    subgraph mock_api_srv ["Mock API Service (Port 5001)"]
        api_srv["Flask/http.server API Engine"]
        gen_data["Faker Data Simulator"]
        mock_s3["Local storage/ files (Mock S3)"]
    end
    
    %% Database
    subgraph postgres_db ["PostgreSQL (Port 5432)"]
        staging_tbl["staging.user_order_log"]
        dwh_dims["mart.d_customer / d_item / d_city / d_calendar"]
        dwh_facts["mart.f_sales"]
        dwh_mart["mart.f_customer_retention"]
    end

    %% Flow links
    init -->|creates schemas & pre-populates calendar| gen
    gen -->|POST /generate_report| api_srv
    api_srv -.->|returns task_id| gen
    
    poll -->|GET /get_report?task_id=...| api_srv
    api_srv -.->|NOT_READY / SUCCESS (report_id)| poll
    
    inc -->|GET /get_increment?report_id=...&date=...| api_srv
    api_srv -.->|returns increment_id| inc
    
    load -->|Downloads CSV file| mock_s3
    load -->|Saves & deduplicates| staging_tbl
    
    staging_tbl --> dim_item
    staging_tbl --> dim_cust
    staging_tbl --> dim_city
    
    dim_item --> fact
    dim_cust --> fact
    dim_city --> fact
    dwh_dims --> fact
    
    fact -->|populates sales facts| dwh_facts
    dwh_facts -->|calculates retention mart| mart
    mart --> dwh_mart
```

---

## Pipeline Optimizations

### 1. Eliminating Top-Level Database Connections (Scheduler Speedup)
* **Problem**: The original DAG file ran `HttpHook.get_connection('http_conn_id')` at the module's top level. In Airflow, the scheduler parses DAG files every few seconds. Executing database/network calls during parsing causes massive CPU spikes and scheduler latency.
* **Optimization**: Moved the connection lookup and credentials mapping inside the operator callback execution context (`python_callable` functions). The scheduler can now parse the DAG file instantly in memory.

### 2. Isolating Credentials & Environment (Zero-Hardcoding)
* **Problem**: Stating direct endpoints or hardcoding credentials prevents code portability and poses security risks.
* **Optimization**: Fully mapped endpoints to Airflow HTTP and Postgres connections. All details are imported automatically upon container start via `connections.yaml`.

### 3. Adding Automatic Schema & Calendar Initialization
* **Problem**: The original migrations expected Postgres schemas and `mart.d_calendar` to pre-exist, which makes replication error-prone.
* **Optimization**: Integrated a DDL setup task (`init_schema`) at the start of the DAG. It checks and builds schemas (`staging`/`mart`), DWH tables, and pre-populates the calendar lookup table (`mart.d_calendar`) for years 2025-2027 using Postgres `generate_series`.

---

## How to Run & Verify Locally

The project includes a lightweight local Mock API and S3 storage emulator to allow immediate execution with zero cloud dependencies.

### Step 1: Pre-generate incremental data files
Generate synthetic orders on your host machine (requires `Faker` and `requirements.txt` installed):
```bash
pip install -r requirements.txt
python mock_api/generate_data.py
```
This generates daily incremental CSV logs inside the `mock_api/storage` folder.

### Step 2: Spin up Airflow & database containers
Start the Docker environment:
```bash
docker compose up -d
```
*Wait ~15-20 seconds. The `airflow-init` container will run DWH migrations and automatically import connections.*

### Step 3: Trigger the DAG
1. Navigate to the Airflow UI at [http://localhost:8080](http://localhost:8080).
2. Log in with **username**: `admin` / **password**: `admin`.
3. Unpause the `customer_retention` DAG.
4. Trigger the DAG.
5. Watch the DAG execution. Airflow will catch up and run the ETL for the last 8 days incrementally, loading staging, updating dimensions, and populating `mart.f_customer_retention`.

### Step 4: Verify DWH Mart records
Connect to your local PostgreSQL instance (`localhost:5432`, db: `de_db`, user: `de_user`, password: `de_password`) and check the retention metrics:
```sql
SELECT * FROM mart.f_customer_retention ORDER BY period_id, item_id;
```
