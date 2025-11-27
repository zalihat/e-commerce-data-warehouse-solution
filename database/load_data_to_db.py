import pandas as pd
from sqlalchemy import create_engine

# ------------------------------
# MySQL connection config
# ------------------------------
user = "user"
password = "password"
host = "127.0.0.1"       # localhost
port = 3306
database = "e_commerce_db"

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")

# ------------------------------
# Utility to load CSV into MySQL
# ------------------------------
def load_csv_to_mysql(csv_file, table_name, engine):
    df = pd.read_csv(csv_file)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"✅ Loaded {len(df)} rows into table '{table_name}'")

# ------------------------------
# Load all tables
# ------------------------------
csv_folder = "data"

tables = [
    "customers",
    "products",
    "orders",
    "order_items",
    "resellers",
    "shipments",
    "payments"
]

for table in tables:
    load_csv_to_mysql(f"{csv_folder}/{table}.csv", table, engine)
