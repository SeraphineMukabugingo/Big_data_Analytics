from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
import happybase
import os
import pandas as pd
import matplotlib.pyplot as plt

# === 1. Initialize Spark ===
spark = SparkSession.builder.appName("CLV").getOrCreate()

# === 2. MongoDB: Total spent per user ===
client = MongoClient('mongodb://localhost:27017/')
transactions = client['ecommerce']['transactions'].aggregate([
    {"$group": {"_id": "$user_id", "total_spent": {"$sum": "$total"}}}
])
mongo_data = [(t["_id"], t["total_spent"]) for t in transactions]
mongo_df = spark.createDataFrame(mongo_data, ["user_id", "total_spent"])
client.close()

# === 3. HBase: Session metrics ===
connection = happybase.Connection('localhost')
table = connection.table('UserSessions')
session_data = []

for row_key, data in table.scan(columns=[b'session_info:duration']):
    try:
        user_id = row_key.decode().split('#')[0]
        duration = int(data[b'session_info:duration'].decode())
        session_data.append((user_id, duration))
    except Exception as e:
        print(f"Skipping malformed row: {row_key}, error: {e}")

connection.close()

# === 4. Convert to DataFrame ===
if session_data:
    hbase_rdd = spark.sparkContext.parallelize(session_data)
    
    # Aggregate: (user_id, session_count, total_duration)
    hbase_agg = hbase_rdd.groupBy(lambda x: x[0]).map(
        lambda x: (x[0], len(x[1]), sum(y[1] for y in x[1]))
    )
    hbase_df = hbase_agg.toDF(["user_id", "session_count", "total_duration"])
else:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    empty_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_count", IntegerType(), True),
        StructField("total_duration", IntegerType(), True),
    ])
    hbase_df = spark.createDataFrame([], schema=empty_schema)

# === 5. Join and compute CLV ===
clv_df = mongo_df.join(hbase_df, "user_id", "left") \
    .fillna({"session_count": 0, "total_duration": 0}) \
    .withColumn("engagement_score", col("session_count") * col("total_duration") / 1000) \
    .withColumn("clv", col("total_spent") * col("engagement_score"))

# === 6. Save CLV results ===
clv_output_dir = "/home/seraphine/Big_Data_Final_project/ecommerce_project/clv_results"
os.makedirs(clv_output_dir, exist_ok=True)
clv_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(clv_output_dir)
clv_df.show(5, truncate=False)

# === 7. Visualization setup ===
csv_path = "/home/seraphine/Big_Data_Final_project/ecommerce_project/sales_data.csv"

if not os.path.exists(csv_path):
    print("sales_data.csv not found. Generating sample data...")
    sample_data = {
        "order_id": [1, 2, 3, 4, 5],
        "user_id": ["u1", "u2", "u1", "u3", "u2"],
        "product": ["Laptop", "Phone", "Tablet", "Laptop", "Phone"],
        "quantity": [1, 2, 1, 1, 3],
        "price": [1000, 500, 300, 1000, 500],
        "total": [1000, 1000, 300, 1000, 1500],
        "order_date": pd.date_range(start="2024-01-01", periods=5, freq='M')
    }
    df = pd.DataFrame(sample_data)
    df.to_csv(csv_path, index=False)
    print(f"Sample sales_data.csv created at: {csv_path}")

# === 8. Load and plot sales trend ===
sales_data = pd.read_csv(csv_path, parse_dates=["order_date"])
sales_by_month = sales_data.groupby(sales_data["order_date"].dt.to_period("M"))["total"].sum()
sales_by_month.index = sales_by_month.index.to_timestamp()

plt.figure(figsize=(10, 5))
plt.plot(sales_by_month.index, sales_by_month.values, marker='o', linestyle='-')
plt.title("Total Sales Over Time")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.grid(True)
plt.tight_layout()

# === 9. Save visualization ===
plot_path = "/home/seraphine/Big_Data_Final_project/ecommerce_project/sales_trend.png"
plt.savefig(plot_path)
plt.show()

# === 10. Stop Spark ===
spark.stop()
