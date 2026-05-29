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

# Verify bronze tables are accessible
all_items = mssparkutils.fs.ls("Tables/")
bronze_tables = [item.name.rstrip("/") for item in all_items if item.name.startswith("bronze_")]
print(f"Found {len(bronze_tables)} Bronze tables:")
for t in bronze_tables:
    print(f"  {t}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders   = spark.read.format("delta").load("Tables/bronze_olist_orders_dataset.csv")
df_items    = spark.read.format("delta").load("Tables/bronze_olist_order_items_dataset.csv")
df_payments = spark.read.format("delta").load("Tables/bronze_olist_order_payments_dataset.csv")

# Aggregate items per order
items_agg = df_items.groupBy("order_id").agg(
    F.count("order_item_id")                              .alias("num_items"),
    F.round(F.sum("price"), 2)                            .alias("total_price"),
    F.round(F.sum("freight_value"), 2)                    .alias("total_freight"),
    F.round(F.sum(F.col("price") + F.col("freight_value")), 2).alias("total_order_value"),
    F.countDistinct("seller_id")                          .alias("num_sellers"),
)

# Aggregate payments per order
payments_agg = df_payments.groupBy("order_id").agg(
    F.round(F.sum("payment_value"), 2)   .alias("payment_total"),
    F.first("payment_type")              .alias("primary_payment_type"),
    F.max("payment_installments")        .alias("max_installments"),
    F.countDistinct("payment_type")      .alias("num_payment_methods"),
)

df_silver_orders = (
    df_orders
    .dropna(subset=["order_id", "customer_id"])
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp",      F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",             F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",  F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
    .withColumn("purchase_date",  F.to_date("order_purchase_timestamp"))
    .withColumn("purchase_year",  F.year("order_purchase_timestamp"))
    .withColumn("purchase_month", F.month("order_purchase_timestamp"))
    .withColumn("is_delivered",   (F.col("order_status") == "delivered").cast("boolean"))
    .withColumn("delivery_days",
        F.when(F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date").cast("date"),
                F.col("order_purchase_timestamp").cast("date")
            )
        )
    )
    .withColumn("is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull(),
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")
        )
    )
    .join(items_agg,    on="order_id", how="left")
    .join(payments_agg, on="order_id", how="left")
    .drop("_ingested_at", "_source_file")
    .withColumn("_silver_at", F.current_timestamp())
)

df_silver_orders.write.format("delta").mode("overwrite").save("Tables/silver_orders")
print(f"✓ silver_orders  ({df_silver_orders.count()} rows)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products    = spark.read.format("delta").load("Tables/bronze_olist_products_dataset.csv")
df_translation = spark.read.format("delta").load("Tables/bronze_product_category_name_translation.csv")
df_items       = spark.read.format("delta").load("Tables/bronze_olist_order_items_dataset.csv")

products_lookup = (
    df_products
    .join(df_translation.select("product_category_name", "product_category_name_english"),
          on="product_category_name", how="left")
    .withColumn("category_english",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")))
    .select("product_id", "category_english", "product_weight_g")
)

df_silver_items = (
    df_items
    .dropna(subset=["order_id", "product_id"])
    .withColumn("price",         F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .withColumn("total_item_value", F.round(F.col("price") + F.col("freight_value"), 2))
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
    .join(products_lookup, on="product_id", how="left")
    .drop("_ingested_at", "_source_file")
    .withColumn("_silver_at", F.current_timestamp())
)

df_silver_items.write.format("delta").mode("overwrite").save("Tables/silver_order_items")
print(f"✓ silver_order_items  ({df_silver_items.count()} rows)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Products
df_silver_products = (
    df_products
    .dropna(subset=["product_id"]).dropDuplicates(["product_id"])
    .withColumn("product_weight_g",  F.col("product_weight_g").cast("double"))
    .withColumn("product_length_cm", F.col("product_length_cm").cast("double"))
    .withColumn("product_height_cm", F.col("product_height_cm").cast("double"))
    .withColumn("product_width_cm",  F.col("product_width_cm").cast("double"))
    .withColumn("volume_cm3",
        F.round(F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"), 2))
    .join(df_translation.select("product_category_name", "product_category_name_english"),
          on="product_category_name", how="left")
    .withColumn("category_english",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")))
    .drop("_ingested_at", "_source_file", "product_category_name_english")
    .withColumn("_silver_at", F.current_timestamp())
)
df_silver_products.write.format("delta").mode("overwrite").save("Tables/silver_products")
print(f"✓ silver_products  ({df_silver_products.count()} rows)")

# Customers
df_silver_customers = (
    spark.read.format("delta").load("Tables/bronze_olist_customers_dataset.csv")
    .dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
    .withColumn("customer_state", F.upper(F.col("customer_state")))
    .withColumn("customer_city",  F.initcap(F.col("customer_city")))
    .drop("_ingested_at", "_source_file")
    .withColumn("_silver_at", F.current_timestamp())
)
df_silver_customers.write.format("delta").mode("overwrite").save("Tables/silver_customers")
print(f"✓ silver_customers  ({df_silver_customers.count()} rows)")

# Sellers
df_silver_sellers = (
    spark.read.format("delta").load("Tables/bronze_olist_sellers_dataset.csv")
    .dropna(subset=["seller_id"]).dropDuplicates(["seller_id"])
    .withColumn("seller_state", F.upper(F.col("seller_state")))
    .withColumn("seller_city",  F.initcap(F.col("seller_city")))
    .drop("_ingested_at", "_source_file")
    .withColumn("_silver_at", F.current_timestamp())
)
df_silver_sellers.write.format("delta").mode("overwrite").save("Tables/silver_sellers")
print(f"✓ silver_sellers  ({df_silver_sellers.count()} rows)")

# Reviews
df_silver_reviews = (
    spark.read.format("delta").load("Tables/bronze_olist_order_reviews_dataset.csv")
    .dropna(subset=["order_id", "review_id"]).dropDuplicates(["review_id"])
    .withColumn("review_score",            F.col("review_score").cast("int"))
    .withColumn("review_creation_date",    F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
    .withColumn("response_hours",
        F.round(
            (F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date")) / 3600, 1
        )
    )
    .drop("_ingested_at", "_source_file")
    .withColumn("_silver_at", F.current_timestamp())
)
df_silver_reviews.write.format("delta").mode("overwrite").save("Tables/silver_reviews")
print(f"✓ silver_reviews  ({df_silver_reviews.count()} rows)")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_items = mssparkutils.fs.ls("Tables/")
silver_tables = [item.name.rstrip("/") for item in all_items if item.name.startswith("silver_")]
print(f"Silver tables ({len(silver_tables)} total):")
for t in silver_tables:
    cnt = spark.read.format("delta").load(f"Tables/{t}").count()
    print(f"  {t}  →  {cnt} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
