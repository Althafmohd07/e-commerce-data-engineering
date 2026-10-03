from pyspark.sql import SparkSession


# ============================================
# E-COMMERCE DATA ENGINEERING PROJECT
# Load Processed Data into PostgreSQL
# ============================================

spark = (
    SparkSession.builder
    .appName("ECommercePostgreSQLLoader")
    .getOrCreate()
)


# --------------------------------------------
# Configuration
# --------------------------------------------

S3_PROCESSED_PATH = "s3a://ecommerce-althaf-2026/processed/"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/ecommerce_db"
POSTGRES_USER = "althafmohd"
POSTGRES_PASSWORD = "Althaf@2026"

JDBC_DRIVER = "org.postgresql.Driver"


# --------------------------------------------
# Read processed data from S3
# --------------------------------------------

print("Reading processed data from S3...")

customers = spark.read.parquet(
    S3_PROCESSED_PATH + "customers/"
)

products = spark.read.parquet(
    S3_PROCESSED_PATH + "products/"
)

orders = spark.read.parquet(
    S3_PROCESSED_PATH + "orders/"
)

order_items = spark.read.parquet(
    S3_PROCESSED_PATH + "order_items/"
)

payments = spark.read.parquet(
    S3_PROCESSED_PATH + "payments/"
)


# --------------------------------------------
# Select required columns
# --------------------------------------------

customers = customers.select(
    "customer_id",
    "customer_name",
    "email",
    "city",
    "state",
    "registration_date"
)

products = products.select(
    "product_id",
    "product_name",
    "category",
    "price"
)

orders = orders.select(
    "order_id",
    "customer_id",
    "order_date",
    "total_quantity",
    "total_revenue"
)

order_items = order_items.select(
    "order_id",
    "product_id",
    "quantity",
    "product_name",
    "category",
    "price",
    "line_total"
)

# Payments actually contains only these 4 columns
payments = payments.select(
    "payment_id",
    "order_id",
    "payment_method",
    "payment_status"
)


# --------------------------------------------
# PostgreSQL JDBC properties
# --------------------------------------------

properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": JDBC_DRIVER
}


# --------------------------------------------
# Load Customers
# --------------------------------------------

print("Loading customers...")

customers.write \
    .mode("append") \
    .jdbc(
        POSTGRES_URL,
        "customers",
        properties=properties
    )

print("Customers loaded successfully")


# --------------------------------------------
# Load Products
# --------------------------------------------

print("Loading products...")

products.write \
    .mode("append") \
    .jdbc(
        POSTGRES_URL,
        "products",
        properties=properties
    )

print("Products loaded successfully")


# --------------------------------------------
# Load Orders
# --------------------------------------------

print("Loading orders...")

orders.write \
    .mode("append") \
    .jdbc(
        POSTGRES_URL,
        "orders",
        properties=properties
    )

print("Orders loaded successfully")


# --------------------------------------------
# Load Order Items
# --------------------------------------------

print("Loading order items...")

order_items.write \
    .mode("append") \
    .jdbc(
        POSTGRES_URL,
        "order_items",
        properties=properties
    )

print("Order items loaded successfully")


# --------------------------------------------
# Load Payments
# --------------------------------------------

print("Loading payments...")

payments.write \
    .mode("append") \
    .jdbc(
        POSTGRES_URL,
        "payments",
        properties=properties
    )

print("Payments loaded successfully")


# --------------------------------------------
# Final validation
# --------------------------------------------

print("")
print("==============================================")
print("   E-COMMERCE POSTGRESQL LOAD COMPLETED")
print("==============================================")

print("Customers:", customers.count())
print("Products:", products.count())
print("Orders:", orders.count())
print("Order Items:", order_items.count())
print("Payments:", payments.count())

print("==============================================")


# --------------------------------------------
# Stop Spark
# --------------------------------------------

spark.stop()