from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# --- Customers ---
def generate_customers(n=500, seed=42):
    Faker.seed(seed)
    random.seed(seed)

    countries = [
        "United States","United Kingdom","Canada","Germany",
        "France","Brazil","Nigeria","India","Australia","Netherlands"
    ]
    start_date = datetime.now() - timedelta(days=365*2)

    rows = []
    for i in range(1, n+1):
        signup_delta_days = random.randint(0, 365*2)
        signup_dt = start_date + timedelta(days=signup_delta_days, seconds=random.randint(0,86400))
        rows.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "signup_date": signup_dt,
            "marketing_opt_in": random.choices([True, False], weights=[0.7,0.3])[0],
            "country": random.choice(countries)
        })

    df = pd.DataFrame(rows)
    df["customer_id"] = df["customer_id"].astype(int)
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["marketing_opt_in"] = df["marketing_opt_in"].astype(bool)
    return df

# --- Products ---
def generate_products(n=200, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    categories = ["Electronics","Books","Toys","Home","Fashion","Sports","Beauty"]
    rows = []
    for i in range(1, n+1):
        rows.append({
            "product_id": i,
            "sku": fake.unique.bothify(text="SKU-####"),
            "name": fake.word().capitalize() + " " + random.choice(categories),
            "category": random.choice(categories),
            "brand": fake.company(),
            "price": round(random.uniform(10,500),2),
            "cost": round(random.uniform(5,250),2),
            "created_at": datetime.now() - timedelta(days=random.randint(0,365)),
            "is_active": random.choice([True, True, True, False])
        })
    return pd.DataFrame(rows)

# --- Orders ---
ORDER_STATUSES = ['pending','paid','shipped','delivered','cancelled','returned']
PAYMENT_METHODS = ['credit_card','paypal','bank_transfer','apple_pay','google_pay']
CHANNELS = ['web','mobile','marketplace']

def generate_orders(customers_df, n_orders=1000, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    np.random.seed(seed)
    orders = []
    start_date = datetime.now() - timedelta(days=365)

    for i in range(1, n_orders+1):
        customer = customers_df.sample(1).iloc[0]
        order_date = start_date + timedelta(days=random.randint(0,365), seconds=random.randint(0,86400))
        status = random.choice(ORDER_STATUSES)
        payment_method = random.choice(PAYMENT_METHODS)
        coupon = random.choice([None,"WELCOME10","SUMMER15","FREESHIP",None,None])
        channel = random.choice(CHANNELS)

        orders.append({
            "order_id": i,
            "customer_id": int(customer.customer_id),
            "order_date": order_date,
            "status": status,
            "total_amount": 0.0,  # will calculate after items
            "payment_method": payment_method,
            "coupon_code": coupon,
            "channel": channel
        })
    return pd.DataFrame(orders)

# --- Order Items ---
# def generate_order_items(orders_df, product_ids, seed=42):
   
#     Faker.seed(seed)
#     random.seed(seed)
#     np.random.seed(seed)

#     items = []
#     order_item_id = 1
#     for _, order in orders_df.iterrows():
#         num_items = random.randint(1,5)
#         for _ in range(num_items):
#             product_id = random.choice(product_ids)
#             quantity = random.randint(1,3)
#             unit_price = round(random.uniform(10,300),2)
#             discount = round(random.uniform(0, unit_price*0.2),2)
#             items.append({
#                 "order_item_id": order_item_id,
#                 "order_id": order.order_id,
#                 "product_id": product_id,
#                 "quantity": quantity,
#                 "unit_price": unit_price,
#                 "discount": discount
#             })
#             order_item_id += 1

#     items_df = pd.DataFrame(items)

#     # Aggregate totals
#     totals = (items_df.assign(net=lambda x: (x.quantity*x.unit_price)-x.discount)
#               .groupby("order_id")["net"].sum().reset_index().rename(columns={"net":"total_amount"}))
#     orders_df = orders_df.merge(totals,on="order_id",how="left")
#     orders_df["total_amount"] = orders_df["total_amount"].fillna(0).round(2)
#     return orders_df, items_df

def generate_order_items(orders_df, product_ids, seed=42):
    import pandas as pd
    import random
    import numpy as np
    from faker import Faker

    Faker.seed(seed)
    random.seed(seed)
    np.random.seed(seed)

    # Ensure total_amount exists
    if "total_amount" not in orders_df.columns:
        orders_df["total_amount"] = 0.0

    items = []
    order_item_id = 1
    for _, order in orders_df.iterrows():
        num_items = random.randint(1,5)
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1,3)
            unit_price = round(random.uniform(10,300),2)
            discount = round(random.uniform(0, unit_price*0.2),2)
            items.append({
                "order_item_id": order_item_id,
                "order_id": order.order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount
            })
            order_item_id += 1

    items_df = pd.DataFrame(items)

    # Aggregate totals
    totals = (
        items_df
        .assign(net=lambda x: (x.quantity*x.unit_price) - x.discount)
        .groupby("order_id")["net"]
        .sum()
        .reset_index()
        .rename(columns={"net":"total_amount"})
    )

    # Merge safely
    orders_df = orders_df.drop(columns=["total_amount"], errors="ignore")  # drop old column if exists
    orders_df = orders_df.merge(totals, on="order_id", how="left")
    orders_df["total_amount"] = orders_df["total_amount"].fillna(0).round(2)

    return orders_df, items_df


# --- Resellers ---
def generate_resellers(n=10, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    rows = []
    countries = ["United States","UK","Canada","Germany","France","Brazil","Nigeria","India","Australia","Netherlands"]
    for i in range(1,n+1):
        rows.append({
            "reseller_id": i,
            "name": fake.company(),
            "country": random.choice(countries),
            "contact_email": fake.company_email()
        })
    return pd.DataFrame(rows)

def assign_resellers(order_items_df, resellers_df, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    order_items_df['reseller_id'] = order_items_df['product_id'].apply(lambda x: random.choice(resellers_df['reseller_id'].tolist()))
    return order_items_df

# --- Shipments ---
def generate_shipments(orders_df, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    shipments = []
    carriers = ["FedEx","DHL","UPS","USPS"]
    shipment_id = 1
    for _, order in orders_df.iterrows():
        shipped_date = order.order_date + pd.to_timedelta(random.randint(1,7), unit='d')
        delivered_date = shipped_date + pd.to_timedelta(random.randint(1,5), unit='d') if random.random()>0.1 else None
        shipments.append({
            "shipment_id": shipment_id,
            "order_id": order.order_id,
            "carrier": random.choice(carriers),
            "tracking_number": fake.unique.bothify(text='TRACK-#####'),
            "shipped_date": shipped_date,
            "delivered_date": delivered_date
        })
        shipment_id += 1
    return pd.DataFrame(shipments)

# --- Payments ---
def generate_payments(orders_df, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    payments = []
    providers = ["Stripe","PayPal","Square","ApplePay","GooglePay"]
    payment_id = 1
    for _, order in orders_df.iterrows():
        payments.append({
            "payment_id": payment_id,
            "order_id": order.order_id,
            "provider": random.choice(providers),
            "amount": order.total_amount,
            "status": random.choices(["pending","success","failed","refunded"], weights=[0.05,0.8,0.1,0.05])[0],
            "transaction_id": fake.unique.bothify(text='TXN-######'),
            "created_at": order.order_date + pd.to_timedelta(random.randint(0,2), unit='d')
        })
        payment_id += 1
    return pd.DataFrame(payments)

import os

# --- Utility to create folder ---
def ensure_data_folder(folder="data"):
    """Create the folder if it doesn't exist."""
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"✅ Created folder: {folder}")
    return folder

# --- Updated CSV saver ---
def save_to_csv(df, filename, folder="data"):
    """Save DataFrame to CSV inside the data folder."""
    folder = ensure_data_folder(folder)
    path = os.path.join(folder, filename)
    df.to_csv(path, index=False)
    print(f"✅ Saved {len(df)} rows to {path}")


# --- Main Execution ---
if __name__ == "__main__":
    # Customers & Products
    customers_df = generate_customers(500)
    products_df = generate_products(200)
    
    # Orders & Items
    orders_df = generate_orders(customers_df, n_orders=1000)
    orders_df, order_items_df = generate_order_items(orders_df, products_df['product_id'].tolist())
    
    # Resellers
    resellers_df = generate_resellers(10)
    order_items_df = assign_resellers(order_items_df, resellers_df)
    
    # Shipments & Payments
    shipments_df = generate_shipments(orders_df)
    payments_df = generate_payments(orders_df)
    
    # Save all CSVs
    save_to_csv(customers_df, "customers.csv")
    save_to_csv(products_df, "products.csv")
    save_to_csv(orders_df, "orders.csv")
    save_to_csv(order_items_df, "order_items.csv")
    save_to_csv(resellers_df, "resellers.csv")
    save_to_csv(shipments_df, "shipments.csv")
    save_to_csv(payments_df, "payments.csv")
