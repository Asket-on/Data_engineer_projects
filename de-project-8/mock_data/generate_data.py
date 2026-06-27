import json
import time
from datetime import datetime
import redis
from confluent_kafka import Producer

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SOURCE_TOPIC = 'order-service-orders'

# Mock Data Definitions
mock_users = {
    "user_01": {"id": "user_01", "name": "Ivanov Ivan"},
    "user_02": {"id": "user_02", "name": "Petrov Petr"},
    "user_03": {"id": "user_03", "name": "Sidorov Sidor"},
}

mock_restaurants = {
    "rest_01": {
        "id": "rest_01",
        "name": "Pizza Palace",
        "menu": [
            {"_id": "prod_01", "name": "Pepperoni Pizza", "category": "Pizza", "price": 500},
            {"_id": "prod_02", "name": "Margarita Pizza", "category": "Pizza", "price": 400},
            {"_id": "prod_03", "name": "Coca Cola", "category": "Drinks", "price": 100},
        ]
    },
    "rest_02": {
        "id": "rest_02",
        "name": "Burger Joint",
        "menu": [
            {"_id": "prod_04", "name": "Cheeseburger", "category": "Burgers", "price": 300},
            {"_id": "prod_05", "name": "French Fries", "category": "Sides", "price": 150},
            {"_id": "prod_06", "name": "Milkshake", "category": "Drinks", "price": 200},
        ]
    }
}

mock_orders = [
    {
        "object_id": 10001,
        "object_type": "order",
        "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": {
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "cost": 1100,
            "payment": 1100,
            "final_status": "CLOSED",
            "restaurant": {"id": "rest_01"},
            "user": {"id": "user_01"},
            "order_items": [
                {"id": "prod_01", "price": 500, "quantity": 2},
                {"id": "prod_03", "price": 100, "quantity": 1}
            ]
        }
    },
    {
        "object_id": 10002,
        "object_type": "order",
        "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": {
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "cost": 450,
            "payment": 450,
            "final_status": "CLOSED",
            "restaurant": {"id": "rest_02"},
            "user": {"id": "user_01"},
            "order_items": [
                {"id": "prod_04", "price": 300, "quantity": 1},
                {"id": "prod_05", "price": 150, "quantity": 1}
            ]
        }
    },
    {
        "object_id": 10003,
        "object_type": "order",
        "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": {
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "cost": 500,
            "payment": 500,
            "final_status": "CLOSED",
            "restaurant": {"id": "rest_01"},
            "user": {"id": "user_02"},
            "order_items": [
                {"id": "prod_01", "price": 500, "quantity": 1}
            ]
        }
    },
    {
        "object_id": 10004,
        "object_type": "order",
        "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": {
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "cost": 650,
            "payment": 650,
            "final_status": "CLOSED",
            "restaurant": {"id": "rest_02"},
            "user": {"id": "user_03"},
            "order_items": [
                {"id": "prod_04", "price": 300, "quantity": 1},
                {"id": "prod_05", "price": 150, "quantity": 1},
                {"id": "prod_06", "price": 200, "quantity": 1}
            ]
        }
    },
    {
        "object_id": 10005,
        "object_type": "order",
        "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": {
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "cost": 400,
            "payment": 400,
            "final_status": "CLOSED",
            "restaurant": {"id": "rest_01"},
            "user": {"id": "user_02"},
            "order_items": [
                {"id": "prod_02", "price": 400, "quantity": 1}
            ]
        }
    }
]

def populate_redis():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Write users
    for user_id, user_data in mock_users.items():
        r.set(user_id, json.dumps(user_data))
        print(f"Set Redis Key: {user_id} -> {user_data}")
        
    # Write restaurants
    for rest_id, rest_data in mock_restaurants.items():
        r.set(rest_id, json.dumps(rest_data))
        print(f"Set Redis Key: {rest_id} -> {rest_data}")
        
    print("Redis population completed.")

def produce_orders():
    print(f"Connecting to Kafka producer at {KAFKA_BOOTSTRAP_SERVERS}...")
    p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    print(f"Publishing {len(mock_orders)} mock orders to topic '{SOURCE_TOPIC}'...")
    for order in mock_orders:
        p.produce(SOURCE_TOPIC, json.dumps(order), callback=delivery_report)
        # Flush to send immediately
        p.flush()
        time.sleep(0.5)
        
    print("Kafka publishing completed.")

if __name__ == "__main__":
    populate_redis()
    produce_orders()
