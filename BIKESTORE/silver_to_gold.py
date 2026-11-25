# Databricks notebook: Silver -> Gold
# Produces: daily sales summary, monthly sales summary, top products by revenue,
# customer lifetime value. Logs rows read/rejected/written at each step.
# Input/output format: CSV throughout

from datetime import datetime
from pyspark.sql import functions as F

STORAGE_ACCOUNT = "gamingstorageset"
CONTAINER = "gamingdata"

SILVER_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Silver"
GOLD_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Gold"

audit_log = []  # collects row counts for every step, written out at the end


def log_step(table_name, read_count, rejected_count, written_count):
    audit_log.append({
        "table": table_name,
        "rows_read": int(read_count),
        "rows_rejected": int(rejected_count),
        "rows_written": int(written_count),
        "run_ts": datetime.utcnow().isoformat(),
    })
    print(f"{table_name}: read={read_count}, rejected={rejected_count}, written={written_count}")


def read_silver(table_path):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SILVER_BASE}/{table_path}/")
    )


def write_gold(df, table_name):
    # coalesce(1) forces a single output file instead of many small part
    # files, which matters a lot when the consumer (e.g. Power BI over a
    # slow/high-latency connection) has to fetch each file individually
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true") \
        .save(f"{GOLD_BASE}/{table_name}/")


# ---------- Load Silver tables ----------
orders = read_silver("sales/orders")
order_items = read_silver("sales/order_items")
products = read_silver("production/products")
customers = read_silver("sales/customers")

orders_count_in = orders.count()
order_items_count_in = order_items.count()

# ---------- Data quality checks (re-applied defensively at Gold too) ----------
# Reject order_items with non-positive quantity or negative/null price
valid_order_items = order_items.filter(
    (F.col("quantity") > 0) &
    (F.col("list_price").isNotNull()) &
    (F.col("list_price") >= 0)
)
rejected_order_items = order_items_count_in - valid_order_items.count()

# Reject orders with missing or invalid order_date
valid_orders = orders.filter(F.col("order_date").isNotNull())
rejected_orders = orders_count_in - valid_orders.count()

# ---------- Build a revenue line-item fact ----------
# revenue per line = quantity * list_price * (1 - discount), discount defaults to 0 if absent
discount_col = F.coalesce(F.col("discount"), F.lit(0.0)) if "discount" in valid_order_items.columns else F.lit(0.0)

line_items = (
    valid_order_items
    .withColumn("discount", discount_col)
    .withColumn("revenue", F.col("quantity") * F.col("list_price") * (1 - F.col("discount")))
    .join(valid_orders.select("order_id", "order_date", "customer_id"), on="order_id", how="inner")
)
line_items.cache()
line_items_count = line_items.count()

# =====================================================================
# 1. Daily sales summary
# =====================================================================
daily_sales_summary = (
    line_items
    .groupBy(F.to_date("order_date").alias("sales_date"))
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("sales_date")
)
write_gold(daily_sales_summary, "daily_sales_summary")
log_step("daily_sales_summary", line_items_count, 0, daily_sales_summary.count())

# =====================================================================
# 2. Monthly sales summary
# =====================================================================
monthly_sales_summary = (
    line_items
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("sales_month"))
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("sales_month")
)
write_gold(monthly_sales_summary, "monthly_sales_summary")
log_step("monthly_sales_summary", line_items_count, 0, monthly_sales_summary.count())

# =====================================================================
# 3. Top products by revenue
# =====================================================================
top_products_by_revenue = (
    line_items
    .groupBy("product_id")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_quantity_sold"),
    )
    .join(products.select("product_id", "product_name"), on="product_id", how="left")
    .select("product_id", "product_name", "total_revenue", "total_quantity_sold")
    .orderBy(F.desc("total_revenue"))
)
write_gold(top_products_by_revenue, "top_products_by_revenue")
log_step("top_products_by_revenue", line_items_count, 0, top_products_by_revenue.count())

# =====================================================================
# 4. Customer lifetime value
# =====================================================================
customer_lifetime_value = (
    line_items
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("revenue"), 2).alias("lifetime_value"),
        F.countDistinct("order_id").alias("total_orders"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .join(customers.select("customer_id", "first_name", "last_name"), on="customer_id", how="left")
    .orderBy(F.desc("lifetime_value"))
)
write_gold(customer_lifetime_value, "customer_lifetime_value")
log_step("customer_lifetime_value", line_items_count, 0, customer_lifetime_value.count())

# ---------- Record rejected rows from the DQ checks above ----------
log_step("orders (dq check)", orders_count_in, rejected_orders, orders_count_in - rejected_orders)
log_step("order_items (dq check)", order_items_count_in, rejected_order_items, order_items_count_in - rejected_order_items)

# ---------- Write the audit log as a Gold table ----------
audit_df = spark.createDataFrame(audit_log)
audit_df.write.format("csv").mode("append").option("header", "true").save(f"{GOLD_BASE}/_audit_log/")

print("\nGold layer complete. Audit log:")
audit_df.show(truncate=False)
