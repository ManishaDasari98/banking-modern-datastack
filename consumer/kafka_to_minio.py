import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import time
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# -----------------------------
# Kafka consumer settings
# -----------------------------
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    # group_id=os.getenv("KAFKA_GROUP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# -----------------------------
# MinIO client
# -----------------------------
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# -----------------------------
# Write function
# -----------------------------
def write_to_minio(table_name, records):
    if not records:
        return

    df = pd.DataFrame(records)
    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    timestamp = datetime.utcnow().strftime('%H%M%S%f')

    file_path = f'{table_name}_{timestamp}.parquet'
    s3_key = f'{table_name}/date={date_str}/{table_name}_{timestamp}.parquet'

    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)

    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# -----------------------------
# Batch + Time config
# -----------------------------
BATCH_SIZE = 10
FLUSH_INTERVAL = 10  # seconds

buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

last_flush_time = time.time()

print("✅ Connected to Kafka. Listening for messages...")

# -----------------------------
# Consume loop
# -----------------------------
for message in consumer:
    topic = message.topic
    event = message.value

    payload = event.get("payload", {})
    record = payload.get("after")

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")

    now = time.time()

    # Flush if batch size reached
    if len(buffer[topic]) >= BATCH_SIZE:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []

    # Time-based flush for low-volume topics
    if now - last_flush_time >= FLUSH_INTERVAL:
        for t, records in buffer.items():
            if records:
                write_to_minio(t.split('.')[-1], records)
                buffer[t] = []
        last_flush_time = now
