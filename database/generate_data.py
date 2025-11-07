import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

fake = Faker()

# -----------------------------------------------------
# Utility: ensure data folder exists
# -----------------------------------------------------
os.makedirs("data", exist_ok=True)


# -----------------------------------------------------
# Utility: add created_at / updated_at to any DataFrame
# -----------------------------------------------------
def add_audit_columns(df):
    now = datetime.now()
    df["created_at"] = [
        (now - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d %H:%M:%S")
        for _ in range(len(df))
    ]
    df["updated_at"] = [
        (now - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
        for _ in range(len(df))
    ]
    return df


# -----------------------------------------------------
# 1️⃣ Customers
# -----------------------------------------------------
def generate_customers(n=500):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "signup_date": fake.date_between(start_date='-1y', end_date='today'),
            "marketing_opt_in": random.choice([True, False]),
            "country": fake.country()
        })
    df = pd.DataFrame(customers)
    df = add_audit_columns(df)
    df.to_csv("data/customers.csv", index=False)
    return df


# -----------------------------------------------------
# 2️⃣ Resellers
# -----------------------------------------------------
def generate_resellers(n=50):
    resellers = []
    for i in range(1, n + 1):
        resellers.append({
            "reseller_id": i,
            "name": fake.company(),
            "contact_email": fake.company_email(),
            "country": fake.country()
        })
    df = pd.DataFrame(resellers)
    df = add_audit_columns(df)
    df.to_csv("data/resellers.csv", index=False)
    return df


# -----------------------------------------------------
# 3️⃣ Products
# -----------------------------------------------------
def generate_products(n=200):
    products = []
    for i in range(1, n + 1):
        products.append({
            "product_id": i,
            "name": fake.catch_phrase(),
            "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"]),
            "price": round(random.uniform(5, 500), 2),
            "reseller_id": random.randint(1, 50)
        })
    df = pd.DataFrame(products)
    df = add_audit_columns(df)
    df.to_csv("data/products.csv", index=False)
    return df


# -----------------------------------------------------
# 4️⃣ Orders + Order Items
# -----------------------------------------------------
def generate_orders(customers_df, n_orders=1000):
    orders = []
    for i in range(1, n_orders + 1):
        orders.append({
            "order_id": i,
            "customer_id": random.choice(customers_df["customer_id"].tolist()),
            "order_date": fake.date_time_between(start_date='-6M', end_date='now'),
            "status": random.choice(['pending', 'paid', 'shipped', 'delivered', 'cancelled', 'returned']),
            "total_amount": round(random.uniform(20, 1500), 2),
            "payment_method": random.choice(['card', 'paypal', 'bank_transfer']),
            "coupon_code": random.choice([None, "DISCOUNT10", "FREESHIP", "WELCOME"]),
            "channel": random.choice(['web', 'mobile', 'marketplace'])
        })
    df = pd.DataFrame(orders)
    df = add_audit_columns(df)
    df.to_csv("data/orders.csv", index=False)
    return df


def generate_order_items(orders_df, products):
    items = []
    for _, order in orders_df.iterrows():
        for _ in range(random.randint(1, 5)):
            product_id = random.choice(products)
            quantity = random.randint(1, 3)
            unit_price = round(random.uniform(5, 500), 2)
            discount = round(random.uniform(0, 50), 2)
            items.append({
                "order_item_id": len(items) + 1,
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount
            })
    df = pd.DataFrame(items)
    df = add_audit_columns(df)
    df.to_csv("data/order_items.csv", index=False)
    return df


# -----------------------------------------------------
# 5️⃣ Shipments
# -----------------------------------------------------
def generate_shipments(orders_df):
    shipments = []
    for i, order in orders_df.iterrows():
        if order["status"] in ["shipped", "delivered"]:
            shipped_date = fake.date_time_between(start_date=order["order_date"])
            delivered_date = shipped_date + timedelta(days=random.randint(1, 7)) \
                if order["status"] == "delivered" else None
            shipments.append({
                "shipment_id": len(shipments) + 1,
                "order_id": order["order_id"],
                "carrier": random.choice(["DHL", "FedEx", "UPS", "USPS"]),
                "tracking_number": fake.uuid4(),
                "shipped_date": shipped_date,
                "delivered_date": delivered_date
            })
    df = pd.DataFrame(shipments)
    df = add_audit_columns(df)
    df.to_csv("data/shipments.csv", index=False)
    return df


# -----------------------------------------------------
# 6️⃣ Payments
# -----------------------------------------------------
def generate_payments(orders_df):
    payments = []
    for _, order in orders_df.iterrows():
        payments.append({
            "payment_id": len(payments) + 1,
            "order_id": order["order_id"],
            "provider": random.choice(["Stripe", "PayPal", "Adyen"]),
            "amount": order["total_amount"],
            "status": random.choice(["pending", "success", "failed", "refunded"]),
            "transaction_id": fake.uuid4(),
            "created_at": order["order_date"]
        })
    df = pd.DataFrame(payments)
    # ensure updated_at exists too
    df = add_audit_columns(df)
    df.to_csv("data/payments.csv", index=False)
    return df


# -----------------------------------------------------
# 🧠 MAIN EXECUTION
# -----------------------------------------------------
if __name__ == "__main__":
    print("🚀 Generating synthetic e-commerce dataset...")

    customers_df = generate_customers()
    resellers_df = generate_resellers()
    products_df = generate_products()
    orders_df = generate_orders(customers_df)
    order_items_df = generate_order_items(orders_df, products_df["product_id"].tolist())
    shipments_df = generate_shipments(orders_df)
    payments_df = generate_payments(orders_df)

    print("✅ Data generation complete! CSV files saved in /data/")
