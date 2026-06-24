import os
import csv
import random
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from faker import Faker

def generate_mock_data():
    print("Generating mock data for Vertica Data Vault 2.0...")
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(parent_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # 1. Users (100 users)
    users = []
    users_file = os.path.join(data_dir, "users.csv")
    with open(users_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(1, 101):
            reg_dt = datetime(2022, 1, 1) + timedelta(
                days=random.randint(0, 180),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            chat_name = fake.user_name()
            country = fake.country()
            age = random.randint(18, 70)
            writer.writerow([i, chat_name, reg_dt.strftime("%Y-%m-%d %H:%M:%S"), country, age])
            users.append({"id": i, "reg_dt": reg_dt})
    print(f"Generated 100 users at {users_file}")

    # 2. Groups (10 groups)
    groups = []
    groups_file = os.path.join(data_dir, "groups.csv")
    with open(groups_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(1, 11):
            admin = random.choice(users)
            reg_dt = admin["reg_dt"] + timedelta(days=random.randint(1, 30))
            group_name = fake.word().capitalize() + " Group"
            is_private = random.choice([True, False])
            writer.writerow([i, admin["id"], group_name, reg_dt.strftime("%Y-%m-%d %H:%M:%S"), is_private])
            groups.append({"id": i, "admin_id": admin["id"], "reg_dt": reg_dt})
    print(f"Generated 10 groups at {groups_file}")

    # 3. Dialogs / Messages (500 dialogs)
    dialogs_file = os.path.join(data_dir, "dialogs.csv")
    with open(dialogs_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(1, 501):
            sender = random.choice(users)
            receiver = random.choice(users)
            while receiver["id"] == sender["id"]:
                receiver = random.choice(users)
            
            # Message timestamp: after sender registration
            msg_ts = sender["reg_dt"] + timedelta(
                days=random.randint(1, 10),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            message = fake.sentence(nb_words=10)
            
            # message_group: 30% chance it's in a group message
            msg_group = ""
            if random.random() < 0.3:
                grp = random.choice(groups)
                msg_group = grp["id"]
                # Adjust message timestamp to be after group registration
                if msg_ts < grp["reg_dt"]:
                    msg_ts = grp["reg_dt"] + timedelta(minutes=random.randint(10, 1440))
                    
            writer.writerow([
                i, 
                msg_ts.strftime("%Y-%m-%d %H:%M:%S"), 
                sender["id"], 
                receiver["id"], 
                message, 
                msg_group
            ])
    print(f"Generated 500 dialogs at {dialogs_file}")

    # 4. Group Logs (200 logs)
    group_log_file = os.path.join(data_dir, "group_log.csv")
    with open(group_log_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(1, 201):
            grp = random.choice(groups)
            user = random.choice(users)
            user_from = random.choice(users)
            
            event = random.choice(["create", "add", "join", "leave", "kick"])
            # Timestamp: after group registration
            evt_ts = grp["reg_dt"] + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            writer.writerow([
                grp["id"],
                user["id"],
                user_from["id"],
                event,
                evt_ts.strftime("%Y-%m-%d %H:%M:%S")
            ])
    print(f"Generated 200 group logs at {group_log_file}")

    # Upload to local MinIO S3
    upload_to_minio(data_dir, ["users", "groups", "dialogs", "group_log"])

def upload_to_minio(data_dir, file_keys):
    bucket_name = "sprint6"
    endpoint_url = "http://localhost:9000"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    print(f"Connecting to MinIO S3 at {endpoint_url}...")
    try:
        s3 = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Create bucket if not exists
        bucket = s3.Bucket(bucket_name)
        try:
            bucket.meta.client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except Exception:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created bucket '{bucket_name}'.")

        # Upload files
        for key in file_keys:
            filename = f"{key}.csv"
            filepath = os.path.join(data_dir, filename)
            print(f"Uploading {filename} to MinIO...")
            bucket.upload_file(filepath, filename)
        print("All mock files uploaded successfully to local MinIO.")

    except Exception as e:
        print(f"Warning: Could not upload to MinIO. Error: {e}")
        print("Please ensure Docker containers are running (docker compose up -d) and try running this script again.")

if __name__ == "__main__":
    generate_mock_data()
