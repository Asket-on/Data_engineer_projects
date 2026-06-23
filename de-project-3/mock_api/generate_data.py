import os
from datetime import datetime, timedelta
import random
import csv
from faker import Faker

def generate_all_data():
    print("Generating incremental mock CSV files for Mock S3...")
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # Static dimensions
    cities = {
        1: "Bochum",
        2: "Dortmund",
        3: "Essen",
        4: "Duisburg",
        5: "Gelsenkirchen"
    }

    items = {
        10: "Pizza Margherita",
        11: "Burger Classic",
        12: "Caesar Salad",
        13: "Coca-Cola",
        14: "Cappuccino",
        15: "Chocolate Cake"
    }

    item_prices = {
        10: 8.50,
        11: 6.90,
        12: 7.20,
        13: 2.50,
        14: 3.50,
        15: 4.20
    }

    # Generate customer lookup to keep customer profiles consistent across dates
    customers = {}
    for cid in range(100, 201):
        customers[cid] = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "city_id": random.choice(list(cities.keys()))
        }

    # Date range: last 15 days
    today = datetime.today()
    start_date = today - timedelta(days=15)

    base_storage_path = os.path.join("storage", "cohort_4", "Asketr", "project")
    os.makedirs(base_storage_path, exist_ok=True)

    row_id_counter = 1
    for d_offset in range(16):
        current_date = start_date + timedelta(days=d_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        inc_id = f"inc-{current_date.strftime('%Y%m%d')}"
        
        inc_dir = os.path.join(base_storage_path, inc_id)
        os.makedirs(inc_dir, exist_ok=True)
        
        csv_filepath = os.path.join(inc_dir, "user_orders_log_inc.csv")
        
        # Generate 30-50 random orders for this day
        num_orders = random.randint(30, 50)
        
        orders = []
        for _ in range(num_orders):
            order_time = current_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            
            customer_id = random.choice(list(customers.keys()))
            cust = customers[customer_id]
            
            item_id = random.choice(list(items.keys()))
            item_name = items[item_id]
            price = item_prices[item_id]
            
            qty = random.randint(1, 4)
            payment_amount = round(qty * price, 2)
            
            status = random.choices(["shipped", "refunded"], weights=[92, 8], k=1)[0]
            
            orders.append({
                "id": row_id_counter,
                "date_time": order_time.strftime("%Y-%m-%d %H:%M:%S"),
                "city_id": cust["city_id"],
                "city_name": cities[cust["city_id"]],
                "customer_id": customer_id,
                "first_name": cust["first_name"],
                "last_name": cust["last_name"],
                "item_id": item_id,
                "item_name": item_name,
                "quantity": qty,
                "payment_amount": payment_amount,
                "status": status
            })
            row_id_counter += 1

        with open(csv_filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "id", "date_time", "city_id", "city_name", "customer_id",
                "first_name", "last_name", "item_id", "item_name",
                "quantity", "payment_amount", "status"
            ])
            writer.writeheader()
            writer.writerows(orders)

    print(f"Generated incremental data directories inside '{base_storage_path}' successfully.")

if __name__ == "__main__":
    generate_all_data()
