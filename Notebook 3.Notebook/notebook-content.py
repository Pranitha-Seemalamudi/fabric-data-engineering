# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5a62683-b033-46ba-859c-9599ec7f2d65",
# META       "default_lakehouse_name": "Orders_Details",
# META       "default_lakehouse_workspace_id": "1780365b-a44a-4458-b981-81587d5d52c6",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5a62683-b033-46ba-859c-9599ec7f2d65"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_orders    = spark.read.format("delta").load("Tables/silver_orders").filter(F.col("is_delivered") == True)
df_all_orders = spark.read.format("delta").load("Tables/silver_orders")
df_items     = spark.read.format("delta").load("Tables/silver_order_items")
df_customers = spark.read.format("delta").load("Tables/silver_customers")
df_sellers   = spark.read.format("delta").load("Tables/silver_sellers")
df_reviews   = spark.read.format("delta").load("Tables/silver_reviews")

print(f"Delivered orders: {df_orders.count()}")
print(f"Order items:      {df_items.count()}")
print(f"Customers:        {df_customers.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders_with_state = df_orders.join(
    df_customers.select("customer_id", "customer_state"),
    on="customer_id", how="left"
)

gold_state = (
    df_orders_with_state
    .groupBy("customer_state")
    .agg(
        F.round(F.sum("total_order_value"),  2).alias("total_revenue"),
        F.count("order_id")                   .alias("total_orders"),
        F.countDistinct("customer_id")        .alias("unique_customers"),
        F.round(F.avg("total_order_value"),  2).alias("avg_order_value"),
        F.round(F.avg("delivery_days"),      1).alias("avg_delivery_days"),
        F.round(F.sum(F.col("is_late_delivery").cast("int")) /
            F.count("order_id") * 100, 1)      .alias("late_delivery_pct"),
    )
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("_gold_at", F.current_timestamp())
)
gold_state.write.format("delta").mode("overwrite").save("Tables/gold_revenue_by_state")
print("✓ gold_revenue_by_state")
gold_state.orderBy("revenue_rank").show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delivered_order_ids = df_orders.select("order_id")

gold_category = (
    df_items.join(delivered_order_ids, on="order_id", how="inner")
    .groupBy("category_english")
    .agg(
        F.round(F.sum("price"),         2).alias("total_revenue"),
        F.round(F.sum("freight_value"), 2).alias("total_freight"),
        F.count("*")                      .alias("total_items_sold"),
        F.countDistinct("order_id")       .alias("total_orders"),
        F.round(F.avg("price"),         2).alias("avg_item_price"),
    )
    .withColumn("pct_of_total",
        F.round(F.col("total_revenue") /
            F.sum("total_revenue").over(
                Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ) * 100, 1))
    .withColumn("_gold_at", F.current_timestamp())
    .orderBy(F.desc("total_revenue"))
)
gold_category.write.format("delta").mode("overwrite").save("Tables/gold_revenue_by_category")
print("✓ gold_revenue_by_category")
gold_category.show(18)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_monthly = (
    df_orders
    .groupBy("purchase_year", "purchase_month")
    .agg(
        F.count("order_id")                   .alias("monthly_orders"),
        F.round(F.sum("total_order_value"), 2).alias("monthly_revenue"),
        F.countDistinct("customer_id")        .alias("active_customers"),
        F.round(F.avg("total_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("delivery_days"),     1).alias("avg_delivery_days"),
    )
    .withColumn("month_start",
        F.to_date(F.concat_ws("-",
            F.col("purchase_year").cast("string"),
            F.lpad(F.col("purchase_month").cast("string"), 2, "0"),
            F.lit("01"))))
    .withColumn("prev_month_revenue",
        F.lag("monthly_revenue").over(Window.orderBy("purchase_year", "purchase_month")))
    .withColumn("mom_growth_pct",
        F.round((F.col("monthly_revenue") - F.col("prev_month_revenue")) /
            F.col("prev_month_revenue") * 100, 1))
    .drop("prev_month_revenue")
    .withColumn("_gold_at", F.current_timestamp())
    .orderBy("purchase_year", "purchase_month")
)
gold_monthly.write.format("delta").mode("overwrite").save("Tables/gold_monthly_trend")
print("✓ gold_monthly_trend")
gold_monthly.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_reviews = (
    df_reviews
    .join(df_orders.select("order_id", "total_order_value", "delivery_days"),
          on="order_id", how="inner")
    .groupBy("review_score")
    .agg(
        F.count("review_id")                  .alias("total_reviews"),
        F.round(F.avg("total_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("delivery_days"),     1).alias("avg_delivery_days"),
    )
    .withColumn("pct_of_reviews",
        F.round(F.col("total_reviews") /
            F.sum("total_reviews").over(
                Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ) * 100, 1))
    .withColumn("_gold_at", F.current_timestamp())
    .orderBy("review_score")
)
gold_reviews.write.format("delta").mode("overwrite").save("Tables/gold_review_scores")
print("✓ gold_review_scores")
gold_reviews.show()

# Final summary
all_items_list = mssparkutils.fs.ls("Tables/")
gold_tables = [item.name.rstrip("/") for item in all_items_list if item.name.startswith("gold_")]
print(f"\n✓ Gold layer complete — {len(gold_tables)} tables ready:")
for t in gold_tables:
    cnt = spark.read.format("delta").load(f"Tables/{t}").count()
    print(f"  {t}  ({cnt} rows)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_items = mssparkutils.fs.ls("Tables/")
gold_tables = [item.name.rstrip("/") for item in all_items if item.name.startswith("gold_")]
print(f"All gold tables ({len(gold_tables)} total):")
for t in gold_tables:
    cnt = spark.read.format("delta").load(f"Tables/{t}").count()
    print(f"  {t}  ({cnt} rows)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
