from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
import happybase
import os

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

# === 6. Save and show results ===
output_path = "/home/seraphine/Big_Data_Final_project/ecommerce_project/output/clv_results.csv"

# Ensure directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Save as single CSV file
clv_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Show sample output
clv_df.show(5, truncate=False)

# === 7. Stop Spark ===
spark.stop()
