# Data_engineer_projects

Here I added 9 projects which have been made by me during my apprenticeship in Yandex.Practicum. Link for the platform https://practicum.yandex.ru/data-engineer/

## Table of content:

|**Project**| **Topic**|**Task**|**Tools**|**Results**|
|----------|----------|-------|--------|--------|
|**[10. Final]()**|Batch processing|Create a pipeline, which retrieves data from Postgres for the given time period and uploads data extract to the Vertica|Postgres, Airflow, Vertica, Python, Metabase| Stable pipeline has been developed |
|**[9. Micro services]()**|Cloud services |Receive real-time data from the Kafka broker, process and decompose into different layers of the data warehouse|Kafka, Postgres, Redis, kubernetes, Python|Three microservices has been developed |
**[8. Restaurant promotions]()** |Real time data processing|Receive messages from Kafka, process and send to two receivers: a Postgres database and a new topic for the Kafka broker|PySpark, Kafka, Postgres |Created real time data processing pipeline|
**[7. Social network]()** |Data Lake|Create data marts on regular basis in Apache Hadoop file system|PySpark, Hadoop, Airflow |Created 3 spark jobs and scheduled with DAG in Airflow|
**[6. Analytical datawarehouse]()** |Data Vault|Build an analytical storage based on Vertica using Data Vault storage model. |Python, Docker, Vertica, Airflow |Created analytical datawarehouse with 2 layares based on Verica DB|
**[4. Settlements with couriers]()** |ETL pipepline creation|Load data from external API to the local DB with Apache Airflow |Python, Docker, PostgreSQL, MongoDB, Airflow |Scripts for data loading have been created|
**[3. Online store]()** |ETL pipepline creation|Change existing pipeline considering modifications in DB |Python, Docker, PostgreSQL, Airflow |Scripts for data loading have been modified |
**[2. Online store]()** |optimize DB structure |re-build DB schema |Docker, PostgreSQL |Scripts for changing DB structure |
**[1. User Segmentation]()** |datamarts building |Create RFM segmentation in local DB |Docker, PostgreSQL |Scripts for user segmentation have been created|