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

    # Clean existing table before writing
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.shipping CASCADE;")
        conn.commit()

    # Re-run staging table creation
    run_sql_file(conn, "0_create_shipping.sql")

    # Dimensions for simulation
    countries = [
        ("Germany", 0.05),
        ("France", 0.06),
        ("UK", 0.08),
        ("Italy", 0.07),
        ("Spain", 0.05)
    ]
    
    agreements = [
        (1, "AGR-1001", 0.02, 0.015),
        (2, "AGR-1002", 0.03, 0.02),
        (3, "AGR-1003", 0.02, 0.012),
        (4, "AGR-1004", 0.04, 0.025),
        (5, "AGR-1005", 0.05, 0.03),
        (6, "AGR-1006", 0.01, 0.01),
        (7, "AGR-1007", 0.03, 0.018),
        (8, "AGR-1008", 0.04, 0.022),
        (9, "AGR-1009", 0.02, 0.014),
        (10, "AGR-1010", 0.05, 0.028)
    ]

    transfers = [
        ("ground", "truck", 0.02),
        ("ground", "rail", 0.015),
        ("air", "airplane", 0.07),
        ("air", "drone", 0.09),
        ("sea", "ship", 0.01),
        ("sea", "ferry", 0.012)
    ]

    records = []
    start_date = datetime(2023, 1, 1)

    # Generate logs for 1,000 shipments
    for i in range(1, 1001):
        shippingid = 900000000 + i
        saleid = 500000000 + random.randint(1, 100000)
        orderid = 700000000 + random.randint(1, 100000)
        clientid = random.randint(1, 100)
        payment_amount = round(random.uniform(50.0, 3000.0), 2)
        vendorid = random.randint(1, 10)

        # Select random country, agreement, and transfer
        base_country = random.choice(countries)[0]
        ship_c_name, ship_c_base_rate = random.choice(countries)
        
        agr_id, agr_num, agr_rate, agr_comm = random.choice(agreements)
        vendor_agreement_description = f"{agr_id}:{agr_num}:{agr_rate}:{agr_comm}"
        
        tr_type, tr_model, tr_rate = random.choice(transfers)
        shipping_transfer_description = f"{tr_type}:{tr_model}"
        
        # Datetime calculation
        t_start = start_date + timedelta(
            days=random.randint(0, 150),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        plan_days = random.randint(2, 8)
        shipping_plan_datetime = t_start + timedelta(days=plan_days)
        hours_to_plan_shipping = plan_days * 24.0

        # Simulate timeline status transitions
        # Status sequence: booked -> approved -> delivering -> received/returned
        states = [
            ("in_progress", "booked", t_start),
            ("in_progress", "approved", t_start + timedelta(hours=random.randint(1, 12))),
            ("in_progress", "delivering", t_start + timedelta(hours=random.randint(13, 36)))
        ]

        # Final state selection (96% received, 4% returned)
        final_state = random.choices(["recieved", "returned"], weights=[96, 4], k=1)[0]
        final_status = "finished" if final_state == "recieved" else "cancelled"
        
        # 12% probability of delay for received shipments
        if final_state == "recieved" and random.random() < 0.12:
            # Delayed shipment: t_end is after plan date
            t_end = shipping_plan_datetime + timedelta(
                days=random.randint(1, 4),
                hours=random.randint(1, 12)
            )
        else:
            # On-time shipment: t_end is before plan date
            t_end = t_start + timedelta(
                days=random.randint(1, plan_days),
                hours=random.randint(0, 23)
            )

        states.append((final_status, final_state, t_end))

        # Insert log row for each state transition
        for status, state, state_dt in states:
            records.append((
                shippingid,
                saleid,
                orderid,
                clientid,
                payment_amount,
                state_dt,
                random.randint(1, 20), # productid
                f"Package {i} status update", # description
                vendorid,
                "Food & Groceries", # namecategory
                base_country,
                status,
                state,
                shipping_plan_datetime,
                hours_to_plan_shipping,
                shipping_transfer_description,
                tr_rate,
                ship_c_name,
                ship_c_base_rate,
                vendor_agreement_description
            ))

    # Batch insert mock data into postgres staging table
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO public.shipping (
                shippingid, saleid, orderid, clientid, payment_amount, state_datetime,
                productid, description, vendorid, namecategory, base_country, status,
                state, shipping_plan_datetime, hours_to_plan_shipping,
                shipping_transfer_description, shipping_transfer_rate,
                shipping_country, shipping_country_base_rate, vendor_agreement_description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """, records)
        conn.commit()

    print(f"Mock data generated successfully ({len(records)} rows inserted into staging).")

def run_pipeline(conn):
    print("Running DWH normalization pipeline...")
    run_sql_file(conn, "1_DDL.sql")
    run_sql_file(conn, "2_Query.sql")
    run_sql_file(conn, "3_View_Shipping_datamart.sql")
    print("DWH pipeline executed successfully.")

def run_quality_checks(conn):
    print("Running Data Quality Checks...")
    with open("data_quality_check.sql", "r", encoding="utf-8") as f:
        checks_sql = f.read()

    queries = [q.strip() for q in checks_sql.split(";") if q.strip()]
    check_names = [
        "Null Values Check",
        "Range Values Check",
        "Consistency Check",
        "Record Count/Completeness Check"
    ]

    results = []
    with conn.cursor() as cur:
        for idx, query in enumerate(queries):
            cur.execute(query)
            res = cur.fetchone()[0]
            passed = (res == 0)
            status = "PASSED" if passed else "FAILED"
            results.append([check_names[idx], res, status])

    print("\n--- DATA QUALITY CHECK REPORT ---")
    print(tabulate(results, headers=["Check Name", "Invalid Rows", "Status"], tablefmt="grid"))
    print("---------------------------------\n")

    # Print sample mart output
    print("Sample Datamart Outputs (First 10 Shipments):")
    df = pd.read_sql("""
        SELECT 
            shippingid, vendorid, transfer_type, full_day_at_shipping, 
            is_delay, is_shipping_finish, delay_day_at_shipping, 
            payment_amount, round(vat, 2) as vat, round(profit, 2) as profit 
        FROM public.shipping_datamart 
        ORDER BY shippingid 
        LIMIT 10;
    """, conn)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    # Print delay breakdown
    print("\nDelay and Completion Statistics:")
    df_stats = pd.read_sql("""
        SELECT 
            is_shipping_finish,
            is_delay,
            COUNT(*) as shipment_count,
            round(AVG(payment_amount), 2) as avg_payment,
            round(AVG(delay_day_at_shipping)::numeric, 2) as avg_delay_days
        FROM public.shipping_datamart
        GROUP BY is_shipping_finish, is_delay
        ORDER BY is_shipping_finish, is_delay;
    """, conn)
    print(tabulate(df_stats, headers='keys', tablefmt='psql', showindex=False))

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
