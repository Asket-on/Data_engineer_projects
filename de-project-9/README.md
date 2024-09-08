# The 9th Project

### DWH requirements
The purpose of building the DWH. Business requirements:
Architech plans to launch user tagging in the application based on order statistics. For example, a user has ordered pizza 10 times—we assign him the tag “Pizza Lover.”

How we count orders:
- We carry out all calculations only for closed orders with the CLOSED status.

Functional requirements:
- Input data format - JSON

Features of layers:
- In STG - initial data as is.
- In DDS - Data Vault data model.
- In CDM there are two showcases:
	- The first display case is a counter for orders by dishes;
	- The second is a counter of orders by product category.

Non-functional requirements:
The first channel is the flow of orders that goes to Kafka (5 orders per minute).
The second channel is dictionary data (dishes, restaurants, users) that goes to Redis.

PostgreSQL is used as the database. The data processing logic needs to be written in Python; it will be deployed in Kubernetes. The message broker for both input and data exchange between services is Kafka. It is necessary to ensure idempotency in processing messages from the broker.
