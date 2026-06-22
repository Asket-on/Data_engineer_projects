---
updated: 2026-06-22T19:13:47+02:00
---
# DWH Normalization & Logistics Shipping Mart (`de-project-2`)

This project implements a normalized Data Warehouse (DWH) schema and an analytical datamart for a logistics delivery network. It migrates a denormalized event-based staging table into normal forms, optimizing database performance, storage efficiency, and analytical query speeds.

---

## Business Context

An e-commerce logistics platform needs to track and analyze shipping performance. The raw data arrives as a flat, denormalized event log where each row represents a status change of a shipment. To build stable reports on shipping delays, delivery durations, shipping tax (VAT), and vendor commission profit, the raw events are normalized and compiled into a decision-ready data mart (`shipping_datamart`).

---

## DWH Data Lineage & Schema

The pipeline parses composite strings, normalizes dimensions, isolates shipment-level configurations, and tracks delivery status milestones.

```mermaid
erDiagram
    %% Dimensions
    shipping_country_rates {
        serial shipping_country_id PK
        text shipping_country
        numeric shipping_country_base_rate
    }
    shipping_agreement {
        bigint agreementid PK
        text agreement_number
        numeric agreement_rate
        numeric agreement_commission
    }
    shipping_transfer {
        serial transfer_type_id PK
        text transfer_type
        text transfer_model
        numeric shipping_transfer_rate
    }

    %% Facts & States
    shipping_info {
        bigint shippingid PK
        timestamp shipping_plan_datetime
        numeric payment_amount
        bigint vendorid
        int shipping_country_id FK
        int transfer_type_id FK
        bigint agreementid FK
    }
    shipping_status {
        bigint shippingid PK
        text status
        text state
        timestamp shipping_start_fact_datetime
        timestamp shipping_end_fact_datetime
    }

    %% Relationships
    shipping_info ||--|| shipping_status : "tracks status"
    shipping_info }|--|| shipping_country_rates : "applies rates"
    shipping_info }|--|| shipping_transfer : "via method"
    shipping_info }|--|| shipping_agreement : "governed by"
    
    %% Mart View
    shipping_datamart {
        bigint shippingid
        bigint vendorid
        text transfer_type
        double full_day_at_shipping "delivery duration"
        int is_delay
        int is_shipping_finish
        double delay_day_at_shipping
        numeric payment_amount
        numeric vat
        numeric profit
    }
    
    shipping_datamart ..> shipping_info : "consolidates"
    shipping_datamart ..> shipping_status : "consolidates"
```

---

## Performance Optimizations

### 1. Eliminating LAST_VALUE Frame Partition Scans
The original query utilized `LAST_VALUE(...) OVER (PARTITION BY ... ORDER BY state_datetime ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)` to track the latest state of each shipment.
* **Problem**: Specifying window frames forces PostgreSQL to scan partitions up to the end of the partition for each row, resulting in high CPU and disk memory overhead.
* **Optimization**: Replaced with `ROW_NUMBER() OVER (PARTITION BY shippingid ORDER BY state_datetime DESC, id DESC) as rn` in a CTE, filtering by `rn = 1`. This uses standard B-tree index ordering and avoids frame-scan overhead.

### 2. Eliminating DISTINCT Over Window Functions
The original logic executed `SELECT DISTINCT` after running window functions on the entire dataset.
* **Problem**: Window functions are computed for *every raw event row*, and only then are duplicates collapsed via sorting/hashing.
* **Optimization**: By utilizing `rn = 1` filtering, we isolate a single row per shipment *prior* to final projections, avoiding a costly global sorting/de-duplication phase.

### 3. Preventing Joins Fanout via Milestone Aggregations
The original script used separate `LEFT JOIN` subqueries to isolate `booked` and `recieved` timestamps.
* **Problem**: If duplicate state events occur, joining back to raw tables triggers row duplication (fanout).
* **Optimization**: Replaced with a single-pass conditional aggregation block:
  ```sql
  MIN(state_datetime) FILTER (WHERE state = 'booked') AS shipping_start_fact_datetime,
  MAX(state_datetime) FILTER (WHERE state = 'recieved') AS shipping_end_fact_datetime
  ```
  This guarantees exactly one milestone row per `shippingid` without joins.

---

## How to Setup & Run Locally

### Prerequisites
- Docker & Docker Compose
- Python 3.8+

### Step 1: Start PostgreSQL Container
Spin up the database container:
```bash
docker compose up -d
```

### Step 2: Install dependencies
Install Python packages:
```bash
pip install -r requirements.txt
```

### Step 3: Initialize DWH & Run Pipeline
Execute the orchestration script:
```bash
python init_and_run.py
```
This script will:
1. Deploys the staging table schema.
2. Generate 1,000 mock shipments with full status timelines (including delayed and cancelled cases).
3. Normalizes and populates dimensions, info, and status tables.
4. Run automated Data Quality Checks and prints a summary report.

---

## Automated Data Quality Checks

The following validation gates are built into the testing suite (`data_quality_check.sql`):
1. **Null Check**: Ensures critical columns in `shipping_datamart` contain no nulls.
2. **Range Check**: Validates that timestamps, payments, VAT, and profits are non-negative.
3. **Consistency Check**: Validates that DWH math models match raw specifications (`vat` and `profit` recalculation).
4. **Completeness Check**: Asserts that the total number of shipments in the analytical mart matches the raw staging logs.
