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
def get_bucket_name(service_path="terraform/services/s3"):
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


print(ensure_bucket_exists())