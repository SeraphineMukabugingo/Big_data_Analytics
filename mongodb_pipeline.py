from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce']

# Aggregation pipeline: Top 5 products by revenue
pipeline = [
    {"$unwind": "$items"},  # Flatten items array
    {"$group": {
        "_id": "$items.product_id",
        "total_revenue": {"$sum": "$items.subtotal"}
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 5}
]

# Execute pipeline
results = db.transactions.aggregate(pipeline)

# Print results
print("Top 5 Products by Revenue:")
for result in results:
    product = db.products.find_one({"product_id": result["_id"]})
    print(f"Product: {product['name']}, ID: {result['_id']}, Revenue: ${result['total_revenue']:.2f}")

# Close connection
client.close()