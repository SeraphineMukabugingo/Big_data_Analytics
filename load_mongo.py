from pymongo import MongoClient
import json
from pathlib import Path

client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce']

# Clear existing collections
db.users.drop()
db.products.drop()
db.transactions.drop()

# Load data
data_dir = Path("/home/seraphine/Big_Data_Final_project/ecommerce_project/data_json")

# ===================== SCHEMA DEFINITIONS =====================
"""
USERS COLLECTION SCHEMA:
{
  "_id": ObjectId,                 // Auto-generated
  "user_id": "user_000001",        // String (6-digit zero-padded)
  "geo_data": {                    // Location info
    "city": "New York",
    "state": "NY",
    "country": "US"
  },
  "registration_date": ISODate,    // DateTime 
  "last_active": ISODate           // DateTime after registration
}
"""
with open(data_dir / "users.json") as f:
    db.users.insert_many(json.load(f))

def get_subcategory(product, category):
    """Safely get matching subcategory with error handling"""
    if "subcategory_id" not in product:
        return None
        
    try:
        return next(
            sc for sc in category["subcategories"] 
            if sc["subcategory_id"] == product["subcategory_id"]
        )
    except StopIteration:
        return None

"""
PRODUCTS COLLECTION SCHEMA:
{
  "_id": ObjectId,                 // Auto-generated
  "product_id": "prod_00001",      // String (5-digit zero-padded)
  "name": "Wireless Headphones",   // Generated catch phrase
  "category_id": "cat_001",        // Reference to categories
  "base_price": 99.99,             // Float between 5-500
  "current_stock": 150,            // Integer (initial stock 413-644)
  "is_active": true,               // Boolean (95% true)
  "price_history": [               // Array of price changes
    {
      "price": 89.99,
      "date": ISODate
    }
  ],
  "creation_date": ISODate,        // Product creation date
  "category": {                    // EMBEDDED category data
    "category_id": "cat_001",
    "name": "Electronics",
    "subcategory_id": "sub_001_01", // Empty string if missing
    "subcategory_name": "Audio"     // Empty string if missing
  }
}
"""
with open(data_dir / "products.json") as f:
    products = json.load(f)
    categories = json.load(open(data_dir / "categories.json"))
    
    for p in products:
        try:
            cat = next(c for c in categories if c["category_id"] == p["category_id"])
            subcat = get_subcategory(p, cat)
            
            p["category"] = {
                "category_id": cat["category_id"],
                "name": cat["name"],
                "subcategory_id": subcat["subcategory_id"] if subcat else "",
                "subcategory_name": subcat["name"] if subcat else ""
            }
        except StopIteration:
            print(f"Warning: No category found for product {p['product_id']}")
            p["category"] = {
                "category_id": p["category_id"],
                "name": "Unknown Category",
                "subcategory_id": "",
                "subcategory_name": ""
            }
    
    db.products.insert_many(products)

"""
TRANSACTIONS COLLECTION SCHEMA:
{
  "_id": ObjectId,                 // Auto-generated
  "transaction_id": "txn_abc123",  // UUID-based
  "session_id": "sess_abc123",     // Reference to sessions
  "user_id": "user_000001",        // Reference to users
  "timestamp": ISODate,            // Matches session end_time
  "items": [                       // Array of purchased items
    {
      "product_id": "prod_00001",
      "quantity": 2,
      "unit_price": 99.99,
      "subtotal": 199.98
    }
  ],
  "subtotal": 199.98,              // Sum of items
  "discount": 0.00,                // 20% chance of discount
  "total": 199.98,                 // subtotal - discount
  "payment_method": "credit_card", // Enum
  "status": "completed"            // Order status
}
"""
with open(data_dir / "transactions.json") as f:
    db.transactions.insert_many(json.load(f))

print("""
Data successfully loaded with schema:
1. USERS: User profiles with geo data
2. PRODUCTS: Inventory with embedded categories
3. TRANSACTIONS: Orders with line items
""")