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

try:
    files = mssparkutils.fs.ls('Files/')
    print(f"Found {len(files)} items in Files/:")
    for f in files:
        print(f"  {f.name}  ({f.size} bytes)")
except Exception as e:
    print(f"Files/ is empty or not accessible: {e}")
    files = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from datetime import datetime, timedelta

if not files:
    print("No files found — generating sample retail data...")
    random.seed(42)
    regions    = ["North", "South", "East", "West"]
    categories = ["Electronics", "Clothing", "Food", "Books"]
    statuses   = ["Completed"] * 6 + ["Cancelled"] * 1

    products = [
        {"product_id": i, "product_name": f"Product_{i:03d}",
         "category": categories[i % 4], "unit_price": round(10 + i * 8.5, 2)}
        for i in range(1, 31)
    ]

    orders = []
    base_date = datetime(2024, 1, 1)
    for i in range(1, 1001):
        p = products[i % 30]
        qty = random.randint(1, 8)
        orders.append({
            "order_id":      i,
            "order_date":    str((base_date + timedelta(days=i % 365)).date()),
            "customer_id":   random.randint(1, 200),
            "region":        regions[i % 4],
            "product_id":    p["product_id"],
            "product_name":  p["product_name"],
            "category":      p["category"],
            "unit_price":    p["unit_price"],
            "quantity":      qty,
            "gross_revenue": round(p["unit_price"] * qty, 2),
            "discount_pct":  random.choice([0, 5, 10, 15]),
            "status":        random.choice(statuses),
        })

    customers = [
        {"customer_id": i, "customer_name": f"Customer_{i:03d}",
         "region": regions[i % 4], "segment": ["Retail","Wholesale","Online"][i % 3],
         "signup_date": str((base_date - timedelta(days=i * 3)).date())}
        for i in range(1, 201)
    ]

    df_orders    = spark.createDataFrame(orders)
    df_products  = spark.createDataFrame(products)
    df_customers = spark.createDataFrame(customers)

    df_orders.coalesce(1).write.mode("overwrite").option("header",True).csv("Files/raw_orders")
    df_products.coalesce(1).write.mode("overwrite").option("header",True).csv("Files/raw_products")
    df_customers.coalesce(1).write.mode("overwrite").option("header",True).csv("Files/raw_customers")

    files = mssparkutils.fs.ls('Files/')
    print(f"Sample data generated. {len(files)} folders in Files/:")
    for f in files:
        print(f"  {f.name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException

ingested = []

for item in mssparkutils.fs.ls('Files/'):
    name = item.name.rstrip('/')
    path = f"Files/{name}"

    try:
        children = mssparkutils.fs.ls(path)
        first_child = children[0].name if children else ""
    except:
        first_child = name

    if name.endswith('.csv') or first_child.endswith('.csv'):
        df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path)
    elif name.endswith('.parquet') or first_child.endswith('.parquet'):
        df = spark.read.parquet(path)
    elif name.endswith('.json'):
        df = spark.read.json(path)
    else:
        print(f"  Skipping {name} (unsupported format)")
        continue

    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.lit(path))

    table_name = f"bronze_{name.replace('-','_').replace(' ','_').lower()}"

    # Write directly to Tables/ — Fabric auto-registers these for SQL Analytics
    df.write.format("delta").mode("overwrite").save(f"Tables/{table_name}")

    ingested.append((table_name, df.count()))
    print(f"  ✓ {path} → Tables/{table_name}  ({df.count()} rows)")

print(f"\nBronze ingestion complete. {len(ingested)} tables created.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
