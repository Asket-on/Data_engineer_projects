# Data Engineering Portfolio: End-to-End Pipelines & Infrastructure

Welcome to my portfolio of data engineering projects developed during my specialization at [Yandex Practicum](https://practicum.yandex.ru/data-engineer/). 

This repository contains **9 production-grade projects** demonstrating end-to-end data architectures—ranging from real-time stream processing and distributed Big Data lakes to Data Vault 2.0 analytical warehouses and containerised ETL pipelines.

---

## 🛠️ Core Technology Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CE2?style=flat-square&logo=apache-airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)
![Vertica](https://img.shields.io/badge/Vertica-0073C6?style=flat-square&logo=vertica&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=flat-square&logo=mongodb&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-66CCFF?style=flat-square&logo=apache-hadoop&logoColor=white)

---

## ⚡ Portfolio Refactoring Highlights

All projects in this repository have been systematically upgraded to meet **production-grade engineering standards**:
- **Full Containerisation**: Every project features a unified, out-of-the-box `docker-compose.yml` or containerised setup, allowing local deployment and end-to-end verification without external cloud dependencies.
- **Airflow Best Practices**: Eliminated module-scope database connection lookups (`get_conn()`) across all DAGs to resolve scheduler latency and prevent socket exhaustion.
- **Strict Idempotency**: Implemented daily cleanup logic (`DELETE` hooks) in all batch pipelines to prevent duplicate writes during retries or manual task re-runs.
- **Dynamic Parameterisation**: Replaced hardcoded credentials, SSL configurations, and cloud-only hosts with dynamic environment injections and variable overrides (e.g., bypass SSL when executing locally).

---

## 📂 Project Directory & Architecture Map

| Project & Source Code | Architectural Theme | Key Technology Stack | Core Solution & Impact |
| :--- | :--- | :--- | :--- |
| 🎓 **[9. Final Project](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-final)** | Batch ETL & CDM Mart | Vertica, Postgres, Airflow, Metabase | Built a scheduled daily pipeline migrating transaction activity and exchange rates to a Vertica staging layer, generating a `global_metrics` mart for fintech analysis. |
| ☁️ **[8. Food Delivery Cloud DWH](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-8)** | Microservices & Stream Ingestion | Kafka, Postgres, Redis, Docker, Yandex Cloud | Built three data-processing microservices (`stg`, `dds`, `cdm`) consuming raw JSON streams from Kafka, enriching them via Redis dictionaries, and populating a Data Vault DWH. |
| 📊 **[7. Real-Time Spark Streaming](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-7)** | Stream-Static Integration | PySpark, Kafka, Postgres, Structured Streaming | Designed a real-time subscription analytics engine joining live user activity topics from Kafka with static restaurant metadata tables in PostgreSQL. |
| ❄️ **[6. Social Network Data Lake](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-6)** | Distributed Data Lake | PySpark, Hadoop HDFS, Geospatial API | Built a geospatial recommendation data mart that calculates user distances and suggests chat channels based on HDFS actions logs. |
| 🏛️ **[5. Community Conversion DWH](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-5)** | Data Vault 2.0 | Vertica, Postgres, Airflow, S3, MinIO | Implemented an incremental analytical storage system modeled in **Data Vault 2.0** (Hubs, Links, Satellites) inside Vertica, fed by REST-APIs and S3 files. |
| 🚚 **[4. Courier Payments Mart](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-4)** | Slowly Changing Dimensions | Postgres, MongoDB, Airflow, REST-API | Developed an incremental Airflow ETL pipeline fetching order streams from an API, parsing MongoDB payloads, and building a Slowly Changing Dimensions (SCD) model. |
| 🔄 **[3. Customer Retention Pipeline](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-3)** | Incremental ELT | Postgres, Airflow, S3 | Modified and extended an online store pipeline to handle refunds, order cancellations, and customer retention metrics. |
| 🗄️ **[2. OLTP Database Normalisation](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-2)** | Database Re-Design | PostgreSQL, Python | Normalized a large, flat, slow-performing OLTP table into clean dimensional tables (Facts/Dimensions) and optimized query runtime using views. |
| 🏷️ **[1. User RFM Segmentation](https://github.com/Asket-on/Data_engineer_projects/tree/main/de-project-1)** | Analytics & Data Marts | SQL, PostgreSQL | Wrote complex SQL scripts to calculate **Recency, Frequency, and Monetary (RFM)** segments to drive targeted user marketing campaigns. |

---

## 🚀 How to Run Locally
Each project directory contains a dedicated `README.md` with step-by-step instructions. Generally, running any project follows this structure:

1. **Spin Up Infrastructure**:
   Navigate to the project folder and run:
   ```bash
   docker compose up -d
   ```
2. **Execute Ingestion & ETL**:
   Trigger DAGs in the Airflow Web UI (`http://localhost:8080` or `http://localhost:8280`) or run the provided streaming tasks.
3. **Verify Data**:
   Query target databases (Postgres/Vertica/HDFS) using the standard CLI tools (`vsql`, `psql`) or view metrics dashboards.