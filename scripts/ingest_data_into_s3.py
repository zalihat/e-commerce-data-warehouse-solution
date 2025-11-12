import os
import io
import json
import boto3
import subprocess
import pandas as pd
import sqlalchemy
from datetime import datetime
from botocore.exceptions import ClientError

# ===================================================
# 🔧 Configuration
# ===================================================
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "e_commerce_db"

# S3_BUCKET = "ecommerce"
S3_METADATA_PREFIX = "metadata/"  # Folder for state file

STATE_FILE = f"{S3_METADATA_PREFIX}ingestion_state.json"

# ===================================================
# ⚙️ Setup
# ===================================================
engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

s3_client = boto3.client(
    "s3"
)
def get_bucket_name(service_path="../terraform/services/s3"):
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=service_path,
        capture_output=True,
        text=True,
        check=True
    )
    outputs = json.loads(result.stdout)
    return outputs["bucket_name"]["value"]

S3_BUCKET=get_bucket_name()
# Create bucket if not exists
def ensure_bucket_exists():
   
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket '{S3_BUCKET}' does not exist (404 Not Found).")
        elif error_code == 'NoSuchBucket':
            print(f"Bucket '{S3_BUCKET}' does not exist (NoSuchBucket error code).")
        else:
            print(f"An unexpected error occurred: {e}")


ensure_bucket_exists()

# ===================================================
# 🧠 Helper Functions
# ===================================================
def load_state():
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=STATE_FILE)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        else:
            raise

def save_state(state):
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=STATE_FILE,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

def get_last_ingested_time(table_name, state):
    return state.get(table_name, {}).get("last_ingested_at", None)

def update_state(table_name, new_timestamp, state):
    state[table_name] = {"last_ingested_at": new_timestamp}
    save_state(state)

# ===================================================
# 🚀 Ingest Function
# ===================================================
def ingest_table(table_name, backfill=False):
    """Extract table from MySQL and load to S3 (Parquet partitioned by year/month/day)"""
    state = load_state()
    last_ingested_at = get_last_ingested_time(table_name, state)

    print(f"\n🔹 Processing table: {table_name}")

    # Full or incremental
    if backfill or last_ingested_at is None:
        query = f"SELECT * FROM {table_name}"
        print("🧾 Full backfill triggered")
    else:
        query = f"SELECT * FROM {table_name} WHERE updated_at > '{last_ingested_at}'"
        print(f"⏩ Incremental load since {last_ingested_at}")

    df = pd.read_sql(query, engine)
    if df.empty:
        print("✅ No new or updated records.")
        return

    # Add ingestion metadata
    df["ingested_at"] = datetime.utcnow()
    partition_date = datetime.utcnow()
    year, month, day = partition_date.strftime("%Y"), partition_date.strftime("%m"), partition_date.strftime("%d")
    raw_data_prefix = 'bronze'
    # Create partitioned path
    object_prefix = f"{raw_data_prefix}/{table_name}/year={year}/month={month}/day={day}/"
    file_name = f"{table_name}_{partition_date.strftime('%Y%m%d_%H%M%S')}.parquet"
    object_name = f"{object_prefix}{file_name}"

    # Convert to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=object_name,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f"✅ Uploaded {len(df)} rows to S3 -> {object_name}")

    # Update state
    if "updated_at" in df.columns:
        new_max_time = df["updated_at"].max()
        update_state(table_name, str(new_max_time), state)
        print(f"🕒 Updated ingestion state to {new_max_time}")


# ===================================================
# 🏁 Run all tables
# ===================================================
if __name__ == "__main__":
    tables = [
        "customers",
        "resellers",
        "products",
        "orders",
        "order_items",
        "shipments",
        "payments",
    ]

    FULL_BACKFILL = True  # toggle to True to backfill everything

    for table in tables:
        ingest_table(table, backfill=FULL_BACKFILL)

    print("\n🎉 All tables processed successfully.")
