import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType, LongType, StructType

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("generate_mock_data") \
        .getOrCreate()
        
    print("Generating mock cities data...")
    # Australian cities from geo.csv (with corrected coordinates)
    cities_data = [
        (1, "Sydney", -33.865, 151.2094),
        (2, "Melbourne", -37.8136, 144.9631),
        (3, "Brisbane", -27.4678, 153.0281),
        (4, "Perth", -31.9522, 115.8589),
        (5, "Adelaide", -34.9289, 138.6011),
        (6, "Gold Coast", -28.0167, 153.47),
        (7, "Cranbourne", -38.0996, 145.2834),
        (8, "Canberra", -35.2931, 149.1269),
        (9, "Newcastle", -32.9167, 151.75),
        (10, "Wollongong", -34.4331, 150.8831),
        (11, "Geelong", -38.15, 144.35),
        (12, "Hobart", -42.8806, 147.325),
        (13, "Townsville", -19.2564, 146.8183),
        (14, "Ipswich", -27.6167, 152.7667),
        (15, "Cairns", -16.9303, 145.7703),
        (16, "Toowoomba", -27.5667, 151.95),
        (17, "Darwin", -12.4381, 130.8411),
        (18, "Ballarat", -37.55, 143.85),
        (19, "Bendigo", -36.75, 144.2667),
        (20, "Launceston", -41.4419, 147.145),
        (21, "Mackay", -21.1411, 149.1861),
        (22, "Rockhampton", -23.375, 150.5117),
        (23, "Maitland", -32.7167, 151.55),
        (24, "Bunbury", -33.3333, 115.6333)
    ]
    
    cities_schema = StructType([
        StructField("id", LongType(), False),
        StructField("city", StringType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False)
    ])
    
    cities_df = spark.createDataFrame(cities_data, schema=cities_schema)
    cities_df.write.mode("overwrite").parquet("/data/cities/geo")
    print("Mock cities written to /data/cities/geo")
    
    print("Generating mock events data...")
    # We will generate:
    # 1. message events
    # 2. reaction events
    # 3. subscription events
    
    # Event struct schema definition
    event_struct_schema = StructType([
        StructField("message_from", LongType(), True),
        StructField("message_to", LongType(), True),
        StructField("message_ts", TimestampType(), True),
        StructField("reaction_from", LongType(), True),
        StructField("user", LongType(), True),
        StructField("subscription_channel", StringType(), True)
    ])
    
    events_schema = StructType([
        StructField("event_type", StringType(), False),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("date", DateType(), False),
        StructField("event", event_struct_schema, False)
    ])
    
    base_time = datetime(2023, 6, 1, 12, 0, 0)
    
    # Mock scenarios:
    # User 1 & User 2 are in channel "tech", never messaged each other, both in Sydney (< 1km distance).
    # User 3 & User 4 are in channel "music", but User 3 is in Brisbane and User 4 is in Melbourne (> 1km).
    # User 5 is a traveler: goes Sydney -> Melbourne -> Sydney, staying in Sydney > 27 days (home city test).
    
    raw_events = []
    
    # 1. Subscriptions
    # User 1 & 2 subscribe to "tech"
    raw_events.append(("subscription", -33.865, 151.2094, (base_time - timedelta(days=5)).date(), 
                       (None, None, None, None, 1, "tech")))
    raw_events.append(("subscription", -33.8651, 151.2095, (base_time - timedelta(days=5)).date(), 
                       (None, None, None, None, 2, "tech")))
                       
    # User 3 & 4 subscribe to "music"
    raw_events.append(("subscription", -27.4678, 153.0281, (base_time - timedelta(days=5)).date(), 
                       (None, None, None, None, 3, "music")))
    raw_events.append(("subscription", -37.8136, 144.9631, (base_time - timedelta(days=5)).date(), 
                       (None, None, None, None, 4, "music")))
                       
    # 2. Messages
    # User 1 sends message to User 3 (should not prevent recommend with User 2)
    raw_events.append(("message", -33.8650, 151.2094, base_time.date(), 
                       (1, 3, base_time, None, None, None)))
    # User 2 sends message to User 3
    raw_events.append(("message", -33.8652, 151.2096, base_time.date(), 
                       (2, 3, base_time + timedelta(minutes=5), None, None, None)))
                       
    # User 3 sends message to User 4 (they messaged, so they shouldn't be recommended)
    raw_events.append(("message", -27.4678, 153.0281, base_time.date(), 
                       (3, 4, base_time + timedelta(minutes=10), None, None, None)))
                       
    # User 5 travel and home city test:
    # June 1: User 5 in Sydney
    raw_events.append(("message", -33.865, 151.2094, base_time.date(), 
                       (5, 1, base_time, None, None, None)))
    # June 10: User 5 in Melbourne (travels)
    raw_events.append(("message", -37.8136, 144.9631, (base_time + timedelta(days=9)).date(), 
                       (5, 1, base_time + timedelta(days=9), None, None, None)))
    # June 15 to July 15: User 5 stays in Sydney (> 27 days home city test)
    for day in range(14, 45):
        msg_time = base_time + timedelta(days=day)
        raw_events.append(("message", -33.865, 151.2094, msg_time.date(), 
                           (5, 1, msg_time, None, None, None)))
                           
    # 3. Reactions
    raw_events.append(("reaction", -33.865, 151.2094, base_time.date(), 
                       (None, None, None, 1, None, None)))
    raw_events.append(("reaction", -37.8136, 144.9631, (base_time + timedelta(days=9)).date(), 
                       (None, None, None, 2, None, None)))
                       
    events_df = spark.createDataFrame(raw_events, schema=events_schema)
    events_df.write.mode("overwrite").parquet("/data/analytics/events")
    print("Mock events written to /data/analytics/events")
    
    spark.stop()

if __name__ == "__main__":
    main()
