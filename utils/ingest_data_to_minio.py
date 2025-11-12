import os
import io
import json
import pandas as pd
import sqlalchemy
from minio import Minio
from datetime import datetime

# ===================================================
# 🔧 Configuration
# ===================================================
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "e_commerce_db"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "ecommerce"

STATE_FILE = "ingestion_state.json"

# ===================================================
# ⚙️ Setup
# ===================================================
engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

# ===================================================
# 🧠 Helper Functions
# ===================================================
def load_state():
    return json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def get_last_ingested_time(table_name, state):
    return state.get(table_name, {}).get("last_ingested_at", None)

def update_state(table_name, new_timestamp, state):
    state[table_name] = {"last_ingested_at": new_timestamp}
    save_state(state)

# ===================================================
# 🚀 Ingest Function
# ===================================================
def ingest_table(table_name, backfill=False):
    """Extract table from MySQL and load to MinIO (Parquet partitioned by year/month/day)"""
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
    year = partition_date.strftime("%Y")
    month = partition_date.strftime("%m")
    day = partition_date.strftime("%d")
    
    # Create partitioned path
    object_prefix = f"{table_name}/year={year}/month={month}/day={day}/"
    file_name = f"{table_name}_{partition_date.strftime('%Y%m%d_%H%M%S')}.parquet"
    object_name = f"{object_prefix}{file_name}"
    
    # Convert to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Upload to MinIO
    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    
    print(f"✅ Uploaded {len(df)} rows to MinIO -> {object_name}")
    
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
    
    FULL_BACKFILL = False  # toggle to True to backfill everything
    
    for table in tables:
        ingest_table(table, backfill=FULL_BACKFILL)
    
    print("\n🎉 All tables processed successfully.")
