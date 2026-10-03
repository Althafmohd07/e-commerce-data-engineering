import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Number of records
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 1000
NUM_ORDERS = 50000

# Raw data location
RAW_PATH = "data/raw"

os.makedirs(RAW_PATH, exist_ok=True)


# =========================
# CUSTOMERS
# =========================

cities = [
    ("Hyderabad", "Telangana"),
    ("Bangalore", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"),
    ("Vijayawada", "Andhra Pradesh")
]

customers = []

for customer_id in range(1, NUM_CUSTOMERS + 1):

    city, state = random.choice(cities)

    customers.append({
        "customer_id": customer_id,
        "customer_name": f"Customer_{customer_id}",
        "email": f"customer{customer_id}@example.com",
        "city": city,
        "state": state,
        "registration_date": (
            datetime(2024, 1, 1)
            + timedelta(days=random.randint(0, 730))
        ).date()
    })

customers_df = pd.DataFrame(customers)

customers_df.to_csv(
    f"{RAW_PATH}/customers.csv",
    index=False
)


# =========================
# PRODUCTS
# =========================

categories = {
    "Electronics": [
        "Laptop",
        "Smartphone",
        "Tablet",
        "Monitor",
        "Headphones"
    ],
    "Accessories": [
        "Mouse",
        "Keyboard",
        "Webcam",
        "USB Cable",
        "Power Bank"
    ],
    "Home": [
        "Chair",
        "Table",
        "Lamp",
        "Mixer",
        "Cooker"
    ],
    "Fashion": [
        "T-Shirt",
        "Jeans",
        "Shoes",
        "Jacket",
        "Watch"
    ]
}

products = []

for product_id in range(1, NUM_PRODUCTS + 1):

    category = random.choice(list(categories.keys()))

    product_name = random.choice(
        categories[category]
    )

    products.append({
        "product_id": product_id,
        "product_name": f"{product_name}_{product_id}",
        "category": category,
        "price": round(
            random.uniform(200, 100000),
            2
        )
    })

products_df = pd.DataFrame(products)

products_df.to_csv(
    f"{RAW_PATH}/products.csv",
    index=False
)


# =========================
# ORDERS
# =========================

statuses = [
    "Delivered",
    "Delivered",
    "Delivered",
    "Shipped",
    "Processing",
    "Cancelled"
]

orders = []

for order_id in range(1, NUM_ORDERS + 1):

    orders.append({
        "order_id": order_id,
        "customer_id": random.randint(
            1,
            NUM_CUSTOMERS
        ),
        "order_date": (
            datetime(2025, 1, 1)
            + timedelta(days=random.randint(0, 364))
        ).date(),
        "status": random.choice(statuses)
    })

orders_df = pd.DataFrame(orders)

orders_df.to_csv(
    f"{RAW_PATH}/orders.csv",
    index=False
)


# =========================
# ORDER ITEMS
# =========================

order_items = []

for order_id in range(1, NUM_ORDERS + 1):

    number_of_products = random.randint(1, 5)

    selected_products = random.sample(
        range(1, NUM_PRODUCTS + 1),
        number_of_products
    )

    for product_id in selected_products:

        order_items.append({
            "order_id": order_id,
            "product_id": product_id,
            "quantity": random.randint(1, 5)
        })

order_items_df = pd.DataFrame(order_items)

order_items_df.to_csv(
    f"{RAW_PATH}/order_items.csv",
    index=False
)


# =========================
# PAYMENTS
# =========================

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Cash on Delivery"
]

payment_statuses = [
    "Paid",
    "Paid",
    "Paid",
    "Pending",
    "Failed"
]

payments = []

for order_id in range(1, NUM_ORDERS + 1):

    payments.append({
        "payment_id": order_id,
        "order_id": order_id,
        "payment_method": random.choice(
            payment_methods
        ),
        "payment_status": random.choice(
            payment_statuses
        )
    })

payments_df = pd.DataFrame(payments)

payments_df.to_csv(
    f"{RAW_PATH}/payments.csv",
    index=False
)


print("\nE-COMMERCE SOURCE DATA CREATED")
print("--------------------------------")
print(f"Customers   : {len(customers_df)}")
print(f"Products    : {len(products_df)}")
print(f"Orders      : {len(orders_df)}")
print(f"Order Items : {len(order_items_df)}")
print(f"Payments    : {len(payments_df)}")
print("--------------------------------")