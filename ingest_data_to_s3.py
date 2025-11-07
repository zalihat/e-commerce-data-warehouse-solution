import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import BytesIO
import datetime

# ------------------------------
# MySQL config
# ------------------------------
mysql_user = "user"
mysql_password = "password"
mysql_host = "127.0.0.1"
mysql_port = 3306
mysql_db = "e_commerce_db"

engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}")

# ------------------------------
# MinIO config
# ------------------------------
minio_endpoint = "http://127.0.0.1:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin123"
minio_bucket = "ecommerce"

s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key
)

# Ensure bucket exists
if minio_bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=minio_bucket)

# ------------------------------
# Tables and timestamp columns
# ------------------------------
tables = {
    "customers": "signup_date",
    "products": "updated_at",
    "orders": "order_date",
    "order_items": "updated_at",
    "resellers": "updated_at",
    "shipments": "shipped_date",
    "payments": "created_at"
}

# Track last ingested timestamps (simple example: store in a dictionary or DB)
last_ingested = {}  # e.g., {"customers": "2025-01-01 00:00:00"}

# ------------------------------
# Function to load incremental data
# ------------------------------
def load_incremental(table_name, ts_column, start_date=None):
    # Determine query start
    if start_date:
        last_time = start_date
    else:
        last_time = last_ingested.get(table_name, "1970-01-01 00:00:00")

    query = f"""
        SELECT * FROM {table_name}
        WHERE {ts_column} > '{last_time}'
    """
    df = pd.read_sql(query, con=engine)
    if df.empty:
        print(f"No new data for {table_name}")
        return

    # Partition by date (optional)
    partition_date = datetime.datetime.now().strftime("%Y-%m-%d")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=minio_bucket, Key=f"{table_name}/{partition_date}.parquet", Body=buffer)
    print(f"✅ Uploaded {len(df)} rows of {table_name} to MinIO for partition {partition_date}")

    # Update last_ingested
    last_ingested[table_name] = df[ts_column].max()

# ------------------------------
# Run pipeline
# ------------------------------
for table, ts_col in tables.items():
    load_incremental(table, ts_col)
