import psycopg2
from datetime import datetime, timedelta
import random

# Postgres settings
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'student'
PG_PASSWORD = 'student_password'
PG_DB = 'db1'

def get_connection():
    try:
        return psycopg2.connect(
            host='postgres',
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
    except psycopg2.OperationalError:
        return psycopg2.connect(
            host='localhost',
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )

def init_postgres_schema(conn):
    with conn.cursor() as cur:
        # Create transactions table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.transactions (
                operation_id VARCHAR(60) PRIMARY KEY,
                account_number_from INT,
                account_number_to INT,
                currency_code INT,
                country VARCHAR(30),
                status VARCHAR(30),
                transaction_type VARCHAR(30),
                amount INT,
                transaction_dt TIMESTAMP
            );
        """)
        
        # Create currencies table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.currencies (
                date_update TIMESTAMP,
                currency_code INT,
                currency_code_with INT,
                currency_with_div NUMERIC(5, 3),
                PRIMARY KEY (date_update, currency_code, currency_code_with)
            );
        """)
    conn.commit()
    print("PostgreSQL source schemas initialized successfully.")

def populate_currencies(conn):
    start_date = datetime(2022, 10, 1)
    end_date = datetime(2022, 11, 1)
    
    currency_rates = [
        {"code": 840, "div": 1.000},  # USD
        {"code": 978, "div": 1.050},  # EUR
        {"code": 949, "div": 0.054},  # TRY
    ]
    
    current_date = start_date
    records = []
    while current_date < end_date:
        for rate in currency_rates:
            # We generate exchange rate entries for multiple hours or just one entry per day
            # Let's write one entry for 00:00:00 per day
            dt = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            records.append((dt, rate["code"], 420, rate["div"]))
        current_date += timedelta(days=1)
        
    with conn.cursor() as cur:
        # Clear existing
        cur.execute("TRUNCATE TABLE public.currencies CASCADE;")
        
        # Insert
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO public.currencies (date_update, currency_code, currency_code_with, currency_with_div) VALUES %s ON CONFLICT DO NOTHING",
            records
        )
    conn.commit()
    print(f"Populated {len(records)} daily exchange rate records in currencies.")

def populate_transactions(conn):
    start_date = datetime(2022, 10, 1)
    end_date = datetime(2022, 10, 31, 23, 59, 59)
    
    countries = ["USA", "Germany", "Turkey", "UK", "Canada"]
    statuses = ["done", "done", "done", "queued", "blocked", "chargeback"]
    types = ["authorisation", "sbp_incoming", "sbp_outgoing", "transfer_incoming", "transfer_outgoing"]
    currencies = [840, 978, 949]
    
    records = []
    # Let's generate about 500 transactions spread across October
    random.seed(42)
    for i in range(500):
        op_id = f"op_{10000 + i}"
        acc_from = random.randint(1001, 1050)
        acc_to = random.randint(2001, 2050)
        curr = random.choice(currencies)
        country = random.choice(countries)
        status = random.choice(statuses)
        t_type = random.choice(types)
        amount = random.randint(100, 100000) # In minimum unit (cents/kuruş)
        
        # Random timestamp in October 2022
        delta_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
        t_dt = start_date + timedelta(seconds=delta_seconds)
        
        records.append((op_id, acc_from, acc_to, curr, country, status, t_type, amount, t_dt))
        
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.transactions CASCADE;")
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO public.transactions (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt) VALUES %s ON CONFLICT DO NOTHING",
            records
        )
    conn.commit()
    print(f"Populated {len(records)} transactional records in transactions.")

if __name__ == "__main__":
    import psycopg2.extras
    print("Connecting to PostgreSQL source...")
    conn = get_connection()
    try:
        init_postgres_schema(conn)
        populate_currencies(conn)
        populate_transactions(conn)
    finally:
        conn.close()
