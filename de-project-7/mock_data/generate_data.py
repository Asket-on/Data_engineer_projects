import os
import time
import json
import psycopg2
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Load settings from environment variables
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'de')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'jovyan')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'jovyan')

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME_IN = os.environ.get('TOPIC_NAME_IN', 'restaurant_campaigns')
TOPIC_NAME_OUT = os.environ.get('TOPIC_NAME_OUT', 'campaign_triggers')

def verify_and_populate_postgres():
    print("Connecting to PostgreSQL to verify subscribers...")
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        # Verify subscribers count
        cur.execute("SELECT COUNT(*) FROM subscribers_restaurants;")
        count = cur.fetchone()[0]
        print(f"Found {count} subscribers in 'subscribers_restaurants'.")
        
        if count == 0:
            print("Populating initial subscriber data...")
            cur.execute("""
                INSERT INTO subscribers_restaurants (client_id, restaurant_id) VALUES
                ('client_1', 'rest_1'),
                ('client_2', 'rest_1'),
                ('client_3', 'rest_2'),
                ('client_4', 'rest_3'),
                ('client_5', 'rest_1');
            """)
            conn.commit()
            print("Subscriber data populated.")
            
        cur.close()
    except Exception as e:
        print(f"PostgreSQL verification failed: {e}")
    finally:
        if conn:
            conn.close()

def create_kafka_topics():
    print(f"Connecting to Kafka AdminClient at {KAFKA_BOOTSTRAP_SERVERS}...")
    for i in range(10):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            break
        except Exception as e:
            print(f"Waiting for Kafka broker to start (attempt {i+1}/10)... Error: {e}")
            time.sleep(3)
    else:
        print("Could not connect to Kafka AdminClient after 10 attempts.")
        return

    topics = [TOPIC_NAME_IN, TOPIC_NAME_OUT]
    new_topics = []
    
    existing_topics = []
    try:
        existing_topics = admin_client.list_topics()
    except Exception as e:
        print(f"Failed to list topics: {e}")

    for t in topics:
        if t not in existing_topics:
            new_topics.append(NewTopic(name=t, num_partitions=1, replication_factor=1))
            
    if new_topics:
        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print(f"Created Kafka topics: {[t.name for t in new_topics]}")
        except Exception as e:
            print(f"Error creating Kafka topics: {e}")
    else:
        print("Kafka topics already exist.")
        
    admin_client.close()

def generate_campaign_events():
    print(f"Connecting Kafka Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Kafka Producer connection failed: {e}")
        return

    current_time = int(time.time())
    
    # Define campaign events
    # End time needs to be in the future, start time in the past to pass the streaming query filter
    campaigns = [
        {
            "restaurant_id": "rest_1",
            "adv_campaign_id": "camp_001",
            "adv_campaign_content": "Get 50% off on all pizzas at Rest_1 until tonight!",
            "adv_campaign_owner": "Luigi's Pizza",
            "adv_campaign_owner_contact": "luigi@luigispizza.com",
            "adv_campaign_datetime_start": current_time - 300,
            "adv_campaign_datetime_end": current_time + 3600,
            "datetime_created": current_time
        },
        {
            "restaurant_id": "rest_2",
            "adv_campaign_id": "camp_002",
            "adv_campaign_content": "Buy 1 get 1 free burger at Rest_2!",
            "adv_campaign_owner": "Burger Castle",
            "adv_campaign_owner_contact": "contact@burgercastle.com",
            "adv_campaign_datetime_start": current_time - 300,
            "adv_campaign_datetime_end": current_time + 7200,
            "datetime_created": current_time
        },
        {
            "restaurant_id": "rest_3",
            "adv_campaign_id": "camp_003",
            "adv_campaign_content": "Free bubble tea with any meal at Rest_3!",
            "adv_campaign_owner": "Tea Palace",
            "adv_campaign_owner_contact": "info@teapalace.com",
            "adv_campaign_datetime_start": current_time - 300,
            "adv_campaign_datetime_end": current_time + 1800,
            "datetime_created": current_time
        },
        {
            "restaurant_id": "rest_non_existent",
            "adv_campaign_id": "camp_004",
            "adv_campaign_content": "This campaign will be filtered out because no subscriber has this restaurant.",
            "adv_campaign_owner": "Ghost Cafe",
            "adv_campaign_owner_contact": "hello@ghostcafe.com",
            "adv_campaign_datetime_start": current_time - 300,
            "adv_campaign_datetime_end": current_time + 3600,
            "datetime_created": current_time
        },
        {
            "restaurant_id": "rest_1",
            "adv_campaign_id": "camp_expired",
            "adv_campaign_content": "This campaign will be filtered out because it has already expired.",
            "adv_campaign_owner": "Luigi's Pizza",
            "adv_campaign_owner_contact": "luigi@luigispizza.com",
            "adv_campaign_datetime_start": current_time - 7200,
            "adv_campaign_datetime_end": current_time - 300,
            "datetime_created": current_time - 7200
        }
    ]

    print("Publishing campaign events to Kafka...")
    for campaign in campaigns:
        producer.send(TOPIC_NAME_IN, campaign)
        print(f"Sent campaign: {campaign['adv_campaign_id']} for restaurant {campaign['restaurant_id']}")
        time.sleep(0.5)

    producer.flush()
    producer.close()
    print("Successfully generated all mock events.")

def main():
    verify_and_populate_postgres()
    create_kafka_topics()
    generate_campaign_events()

if __name__ == '__main__':
    main()
