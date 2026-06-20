import os
import sys
import time
from datetime import datetime, timedelta
import random
import psycopg2
import pandas as pd
from faker import Faker
from tabulate import tabulate

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "de_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "de_password")
DB_NAME = os.getenv("DB_NAME", "de_db")

def get_connection():
    retries = 30
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"PostgreSQL not ready yet, retrying... ({retries} left). Error: {e}")
            time.sleep(2)
            retries -= 1
    print("Could not connect to PostgreSQL. Exiting.")
    sys.exit(1)

def run_sql_file(conn, file_path):
    print(f"Executing SQL file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def generate_mock_data(conn):
    print("Generating mock data...")
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    with conn.cursor() as cur:
        # 1. Clean existing tables if they exist
        cur.execute("DROP SCHEMA IF EXISTS production CASCADE;")
        cur.execute("CREATE SCHEMA production;")
        
        # 2. Create production tables
        cur.execute("""
            CREATE TABLE production.users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                login VARCHAR(50) NOT NULL UNIQUE
            );
            CREATE TABLE production.products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
            );
            CREATE TABLE production.orderstatuses (
                id INT PRIMARY KEY,
                key VARCHAR(20) NOT NULL UNIQUE
            );
            CREATE TABLE production.orders (
                order_id INT PRIMARY KEY,
                order_ts TIMESTAMP NOT NULL,
                user_id INT REFERENCES production.users(id),
                bonus_payment NUMERIC(10, 2) NOT NULL DEFAULT 0,
                payment NUMERIC(10, 2) NOT NULL DEFAULT 0,
                cost NUMERIC(10, 2) NOT NULL DEFAULT 0,
                bonus_grant NUMERIC(10, 2) NOT NULL DEFAULT 0,
                status INT REFERENCES production.orderstatuses(id)
            );
            CREATE TABLE production.orderstatuslog (
                id SERIAL PRIMARY KEY,
                order_id INT NOT NULL,
                status_id INT REFERENCES production.orderstatuses(id),
                dttm TIMESTAMP NOT NULL
            );
            CREATE TABLE production.orderitems (
                id SERIAL PRIMARY KEY,
                product_id INT REFERENCES production.products(id),
                order_id INT REFERENCES production.orders(order_id),
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                discount NUMERIC(10, 2) NOT NULL DEFAULT 0,
                quantity INT NOT NULL CHECK (quantity > 0)
            );
        """)
        conn.commit()

        # 3. Populate orderstatuses
        statuses = [
            (1, "Created"),
            (2, "Processing"),
            (3, "Delivery"),
            (4, "Closed"),
            (5, "Cancelled")
        ]
        cur.executemany("INSERT INTO production.orderstatuses (id, key) VALUES (%s, %s);", statuses)

        # 4. Populate users
        users = []
        for i in range(1, 101):
            users.append((i, fake.name(), fake.user_name() + str(i)))
        cur.executemany("INSERT INTO production.users (id, name, login) VALUES (%s, %s, %s);", users)

        # 5. Populate products
        products = []
        product_names = [
            "Pizza Margherita", "Pizza Pepperoni", "Pizza BBQ Chicken", "Burger Classic",
            "Cheeseburger", "French Fries", "Chicken Nuggets", "Caesar Salad",
            "Greek Salad", "Coca-Cola", "Sprite", "Fanta", "Orange Juice",
            "Apple Pie", "Chocolate Cake", "Ice Cream Vanila", "Cappuccino",
            "Espresso", "Green Tea", "Still Water"
        ]
        for idx, name in enumerate(product_names, 1):
            products.append((idx, name, round(random.uniform(2.0, 15.0), 2)))
        cur.executemany("INSERT INTO production.products (id, name, price) VALUES (%s, %s, %s);", products)
        conn.commit()

        # 6. Populate orders and logs
        start_date = datetime(2022, 1, 1)
        orders = []
        status_logs = []
        order_items = []
        
        # We want to make sure some users have no orders to test RIGHT JOIN completeness
        active_users = list(range(1, 91)) # 90 users have orders, 10 have 0 orders
        
        item_id_counter = 1
        for order_id in range(1, 1001):
            user_id = random.choice(active_users)
            order_ts = start_date + timedelta(
                days=random.randint(0, 360),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # Simulate items
            num_items = random.randint(1, 4)
            order_cost = 0.0
            order_items_batch = []
            for _ in range(num_items):
                prod = random.choice(products)
                prod_id, prod_name, prod_price = prod
                qty = random.randint(1, 3)
                disc = round(random.choice([0.0, 0.1, 0.2]) * prod_price, 2)
                item_price = prod_price - disc
                order_cost += item_price * qty
                
                order_items_batch.append((prod_id, order_id, prod_name, prod_price, disc, qty))
            
            order_cost = round(order_cost, 2)
            bonus_payment = round(random.choice([0.0, 0.0, 0.0, 0.1]) * order_cost, 2)
            payment = round(order_cost - bonus_payment, 2)
            bonus_grant = round(order_cost * 0.05, 2)
            
            final_status = random.choice([4, 4, 4, 4, 4, 4, 4, 5]) # 87.5% success (status 4), 12.5% cancelled (status 5)
            
            orders.append((order_id, order_ts, user_id, bonus_payment, payment, order_cost, bonus_grant, final_status))
            
            # Generate status transition logs
            # Created -> Processing -> Delivery -> Closed/Cancelled
            t_created = order_ts
            t_processing = t_created + timedelta(minutes=random.randint(5, 15))
            t_delivery = t_processing + timedelta(minutes=random.randint(15, 45))
            t_final = t_delivery + timedelta(minutes=random.randint(15, 60))
            
            status_logs.append((order_id, 1, t_created))
            status_logs.append((order_id, 2, t_processing))
            status_logs.append((order_id, 3, t_delivery))
            status_logs.append((order_id, final_status, t_final))
            
            order_items.extend(order_items_batch)
            
        cur.executemany("""
            INSERT INTO production.orders 
            (order_id, order_ts, user_id, bonus_payment, payment, cost, bonus_grant, status) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, orders)
        
        cur.executemany("""
            INSERT INTO production.orderstatuslog (order_id, status_id, dttm) 
            VALUES (%s, %s, %s);
        """, status_logs)
        
        cur.executemany("""
            INSERT INTO production.orderitems (product_id, order_id, name, price, discount, quantity) 
            VALUES (%s, %s, %s, %s, %s, %s);
        """, order_items)
        
        conn.commit()
    print("Mock data generated successfully.")

def run_pipeline(conn):
    print("Initializing analysis schema and views...")
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS analysis;")
        conn.commit()
        
    # Run views
    run_sql_file(conn, "views.sql")
    run_sql_file(conn, "orders_view.sql")
    
    # Run Target DDL
    run_sql_file(conn, "datamart_ddl.sql")
    
    # Run Target Population query
    run_sql_file(conn, "datamart_query.sql")
    
    print("Pipeline executed successfully.")

def run_quality_checks(conn):
    print("Running Data Quality Checks...")
    with open("data_quality_check.sql", "r", encoding="utf-8") as f:
        checks_sql = f.read()
        
    queries = [q.strip() for q in checks_sql.split(";") if q.strip()]
    check_names = [
        "Null Values Check",
        "Range Values Check",
        "Record Count/Completeness Check"
    ]
    
    results = []
    with conn.cursor() as cur:
        for idx, query in enumerate(queries):
            cur.execute(query)
            res = cur.fetchone()[0]
            
            # The check queries return the count of invalid records. If 0, it passes.
            # Except completeness check, where we verify count = 0 (which means diff is 0).
            passed = (res == 0)
            status = "PASSED" if passed else "FAILED"
            results.append([check_names[idx], res, status])
            
    print("\n--- DATA QUALITY CHECK REPORT ---")
    print(tabulate(results, headers=["Check Name", "Invalid Rows", "Status"], tablefmt="grid"))
    print("---------------------------------\n")
    
    # Print sample mart output
    print("Sample Datamart Outputs (First 10 Users):")
    df = pd.read_sql("SELECT * FROM analysis.dm_rfm_segments ORDER BY user_id LIMIT 10;", conn)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
    
    # Print segment distribution matrix
    print("\nRFM Segment Distribution Matrix:")
    df_dist = pd.read_sql("""
        SELECT recency, frequency, COUNT(*) as user_count 
        FROM analysis.dm_rfm_segments 
        GROUP BY recency, frequency 
        ORDER BY recency, frequency;
    """, conn)
    pivot_df = df_dist.pivot(index='recency', columns='frequency', values='user_count').fillna(0).astype(int)
    print(tabulate(pivot_df, headers='keys', tablefmt='psql'))

def main():
    conn = get_connection()
    try:
        generate_mock_data(conn)
        run_pipeline(conn)
        run_quality_checks(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
