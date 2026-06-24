import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker

def generate_mock_data():
    print("Generating mock data for couriers and deliveries...")
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # Determine paths relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    storage_dir = os.path.join(script_dir, "storage")
    os.makedirs(storage_dir, exist_ok=True)

    # 1. Generate 10 couriers
    couriers = []
    for i in range(1, 11):
        couriers.append({
            "_id": f"c-{100 + i}",
            "name": fake.name()
        })

    couriers_file = os.path.join(storage_dir, "couriers.json")
    with open(couriers_file, "w", encoding="utf-8") as f:
        json.dump(couriers, f, ensure_ascii=False, indent=4)
    print(f"Generated {len(couriers)} couriers and saved to {couriers_file}.")

    # 2. Generate 200 deliveries for the last 10 days
    deliveries = []
    today = datetime.today()
    start_date = today - timedelta(days=10)

    for i in range(1, 201):
        # Order timestamp
        order_ts = start_date + timedelta(
            days=random.randint(0, 9),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Delivery timestamp (30 to 120 minutes later)
        delivery_ts = order_ts + timedelta(minutes=random.randint(30, 120))
        
        courier = random.choice(couriers)
        
        rate = random.choices([5, 4, 3, 2, 1], weights=[70, 15, 8, 5, 2], k=1)[0]
        order_sum = round(random.uniform(150.0, 4500.0), 2)
        
        # Tip sum: usually 5% - 15% of sum, sometimes 0
        if random.random() < 0.15:
            tip_sum = 0.0
        else:
            tip_sum = round(order_sum * random.uniform(0.05, 0.15), 2)

        deliveries.append({
            "order_id": f"ord-{1000 + i}",
            "order_ts": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "delivery_id": f"del-{2000 + i}",
            "courier_id": courier["_id"],
            "delivery_ts": delivery_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "rate": rate,
            "sum": order_sum,
            "tip_sum": tip_sum
        })

    deliveries_file = os.path.join(storage_dir, "deliveries.json")
    with open(deliveries_file, "w", encoding="utf-8") as f:
        json.dump(deliveries, f, ensure_ascii=False, indent=4)
    print(f"Generated {len(deliveries)} deliveries and saved to {deliveries_file}.")

if __name__ == "__main__":
    generate_mock_data()
