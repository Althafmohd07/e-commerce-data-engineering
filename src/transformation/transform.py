from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("ECommerceTransformation") \
    .getOrCreate()


# ============================================================
# 2. S3 PATHS
# ============================================================

RAW_PATH = "s3a://ecommerce-althaf-2026/raw/"
PROCESSED_PATH = "s3a://ecommerce-althaf-2026/processed/"


# ============================================================
# 3. READ RAW DATA FROM S3
# ============================================================

customers = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RAW_PATH + "customers.csv")

products = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RAW_PATH + "products.csv")

orders = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RAW_PATH + "orders.csv")

order_items = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RAW_PATH + "order_items.csv")

payments = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RAW_PATH + "payments.csv")


# ============================================================
# 4. REMOVE DUPLICATES
# ============================================================

customers = customers.dropDuplicates(["customer_id"])

products = products.dropDuplicates(["product_id"])

orders = orders.dropDuplicates(["order_id"])

# order_items.csv does NOT have order_item_id.
# Therefore use order_id + product_id.
order_items = order_items.dropDuplicates(
    ["order_id", "product_id"]
)

payments = payments.dropDuplicates(["payment_id"])


# ============================================================
# 5. CONVERT DATE COLUMNS
# ============================================================

customers = customers.withColumn(
    "registration_date",
    to_date(col("registration_date"))
)

orders = orders.withColumn(
    "order_date",
    to_date(col("order_date"))
)


# ============================================================
# 6. JOIN ORDER ITEMS WITH PRODUCTS
#    Get product information and price
# ============================================================

order_items_enriched = order_items.join(
    products,
    order_items.product_id == products.product_id,
    "left"
).select(
    order_items["*"],
    products["product_name"],
    products["category"],
    products["price"]
)


# ============================================================
# 7. CALCULATE LINE TOTAL
#    quantity × product price
# ============================================================

order_items_enriched = order_items_enriched.withColumn(
    "line_total",
    col("quantity") * col("price")
)


# ============================================================
# 8. CALCULATE ORDER-LEVEL REVENUE
# ============================================================

order_revenue = order_items_enriched.groupBy(
    "order_id"
).agg(
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("line_total").alias("total_revenue")
)


# ============================================================
# 9. JOIN ORDERS WITH CUSTOMERS
# ============================================================

orders_enriched = orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "left"
).select(
    orders["*"],
    customers["customer_name"],
    customers["email"],
    customers["city"],
    customers["state"],
    customers["registration_date"]
)


# ============================================================
# 10. JOIN ORDERS WITH ORDER REVENUE
# ============================================================
# Using "order_id" as the join column prevents duplicate
# order_id columns in the final DataFrame.

orders_final = orders_enriched.join(
    order_revenue,
    "order_id",
    "left"
)


# ============================================================
# 11. WRITE PROCESSED DATA TO S3 AS PARQUET
# ============================================================

orders_final.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "orders/")

order_items_enriched.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "order_items/")

payments.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "payments/")

customers.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "customers/")

products.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "products/")


# ============================================================
# 12. SUCCESS MESSAGE
# ============================================================

print("")
print("==============================================")
print("   E-COMMERCE PYSPARK TRANSFORMATION DONE")
print("==============================================")
print("Processed data written to:")
print(PROCESSED_PATH)
print("==============================================")


# ============================================================
# 13. STOP SPARK
# ============================================================

spark.stop()