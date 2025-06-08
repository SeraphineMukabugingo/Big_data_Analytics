import json
import random
import datetime
import uuid
import threading
import numpy as np
from faker import Faker
import logging
import resource
import gc
from pathlib import Path
import shutil
import sys

# Setup logging with DEBUG level for diagnostics
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("dataset_generator.log")
    ]
)
logger = logging.getLogger(__name__)

# Attempt to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not installed. Memory usage monitoring disabled.")

# --- Memory Optimization ---
MEMORY_LIMIT = 16 * 1024 * 1024 * 1024  # 16GB
try:
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
    logger.info("Memory limit set to 16GB")
except ValueError as e:
    logger.warning(f"Could not set memory limit: {e}")

# --- Configuration ---
NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 500000
NUM_SESSIONS = 2000000
TIMESPAN_DAYS = 90
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe
SESSION_BATCH_SIZE = 100000  # Produces ~20 session files
TRANSACTION_BATCH_SIZE = 10000  # Transactions per batch write

# --- Initialization ---
np.random.seed(42)
random.seed(42)
Faker.seed(42)
fake = Faker()

# Data directory
data_dir = Path("/home/seraphine/Big_Data_Final_project/ecommerce_project/data_json")
try:
    # Clear existing data directory to avoid conflicts
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created/cleared data directory: {data_dir}")
except Exception as e:
    logger.error(f"Failed to create/clear data directory: {data_dir}")
    sys.exit(1)

logger.info("Initializing dataset generation...")

# --- ID Generators ---
def generate_session_id():
    """Generate a unique session ID."""
    return f"sess_{uuid.uuid4().hex[:10]}"

def generate_transaction_id():
    """Generate a unique transaction ID."""
    return f"txn_{uuid.uuid4().hex[:12]}"

# --- Inventory Management ---
class InventoryManager:
    """Manages product inventory with thread-safe stock updates."""
    def __init__(self, products):
        """Initialize with a list of products."""
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()
        logger.debug(f"Initialized InventoryManager with {len(self.products)} products")

    def update_stock(self, product_id, quantity):
        """Update product stock, allowing zero stock if insufficient."""
        with self.lock:
            if product_id not in self.products:
                logger.debug(f"Product {product_id} not found")
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            logger.debug(f"Insufficient stock {product_id}: {self.products[product_id]['current_stock']} < {quantity}. Setting to 0.")
            self.products[product_id]["current_stock"] = 0
            return True

    def get_product(self, product_id):
        """Retrieve a product by ID."""
        with self.lock:
            return self.products.get(product_id)

# --- Helper Functions ---
def determine_page_type(position, page_views):
    """Determine the next page type based on position and previous pages."""
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not page_views:
        return "home"
    prev_page = page_views[-1]["page_type"]
    transitions = {
        "home": (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
        "category_listing": (["product_detail", "category_listing", "search", "home"], [0.7, 0.1, 0.1, 0.1]),
        "search": (["product_detail", "search", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "product_detail": (["product_detail", "cart", "category_listing", "search", "home"], [0.3, 0.3, 0.2, 0.1, 0.1]),
        "cart": (["checkout", "product_detail", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "checkout": (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
        "confirmation": (["home", "product_detail", "category_listing"], [0.6, 0.2, 0.2])
    }
    options, weights = transitions.get(prev_page, (["home"], [1.0]))
    return random.choices(options, weights=weights)[0]

def get_page_content(page_type, products_list, categories_list):
    """Generate content for a page based on its type."""
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category_id = product["category_id"]
                category = next((c for c in categories_list if c["category_id"] == category_id), None)
                if category:
                    return product, category
            attempts += 1
        product = random.choice(products_list)
        category_id = product["category_id"]
        category = next((c for c in categories_list if c["category_id"] == category_id), None)
        return product, category
    elif page_type == "category_listing":
        return None, random.choice(categories_list)
    return None, None

def log_memory_usage():
    """Log current memory usage if psutil is available."""
    if PSUTIL_AVAILABLE:
        process = psutil.Process()
        mem_info = process.memory_info()
        logger.debug(f"Memory usage: {mem_info.rss / 1024 / 1024:.2f} MB")

def save_json_file(file_path, data):
    """Save data to a JSON file with error handling."""
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date)) else None)
        logger.info(f"Saved {file_path}")
    except Exception as e:
        logger.error(f"Failed to save {file_path}: {e}")
        raise

# --- Category Generation ---
try:
    categories = []
    for cat_id in range(NUM_CATEGORIES):
        category = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": [
                {
                    "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                    "name": fake.bs(),
                    "profit_margin": round(random.uniform(0.1, 0.4), 2)
                }
                for sub_id in range(random.randint(3, 5))
            ]
        }
        categories.append(category)
    logger.info(f"Generated {len(categories)} categories")
    save_json_file(data_dir / "categories.json", categories)
except Exception as e:
    logger.error(f"Category generation or saving failed: {e}")
    sys.exit(1)

# --- Product Generation ---
try:
    products = []
    product_creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)

    for prod_id in range(NUM_PRODUCTS):
        category = random.choice(categories)
        base_price = round(random.uniform(5, 500), 2)
        price_history = []
        initial_date = fake.date_time_between(
            start_date=product_creation_start,
            end_date=product_creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3)
        )
        price_history.append({"price": base_price, "date": initial_date.isoformat()})
        for _ in range(random.randint(0, 2)):
            price_change_date = fake.date_time_between(start_date=initial_date, end_date="now")
            new_price = round(base_price * random.uniform(0.8, 1.2), 2)
            price_history.append({"price": new_price, "date": price_change_date.isoformat()})
            initial_date = price_change_date
        price_history.sort(key=lambda x: x["date"])
        current_price = price_history[-1]["price"]
        products.append({
            "product_id": f"prod_{prod_id:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": category["category_id"],
            "base_price": current_price,
            "current_stock": random.randint(413, 644),  # Adjusted to match expected remaining stock
            "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
            "price_history": price_history,
            "creation_date": price_history[0]["date"]
        })
    logger.info(f"Generated {len(products)} products")
    save_json_file(data_dir / "products.json", products)
except Exception as e:
    logger.error(f"Product generation or saving failed: {e}")
    sys.exit(1)

# --- User Generation ---
try:
    users = []
    for user_id in range(NUM_USERS):
        reg_date = fake.date_time_between(
            start_date=f"-{TIMESPAN_DAYS * 3}d",
            end_date=f"-{TIMESPAN_DAYS}d"
        )
        users.append({
            "user_id": f"user_{user_id:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code()
            },
            "registration_date": reg_date.isoformat(),
            "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat()
        })
    logger.info(f"Generated {len(users)} users")
    save_json_file(data_dir / "users.json", users)
except Exception as e:
    logger.error(f"User generation or saving failed: {e}")
    sys.exit(1)

# --- Session Generation ---
try:
    inventory = InventoryManager(products)
    session_counter = 0
    session_batch = []
    iteration = 0

    logger.info("Generating sessions...")

    while session_counter < NUM_SESSIONS and iteration < MAX_ITERATIONS:
        iteration += 1
        user = random.choice(users)
        session_id = generate_session_id()
        session_start = fake.date_time_between(
            start_date=f"-{TIMESPAN_DAYS}d",
            end_date="now"
        )
        session_duration = random.randint(30, 3600)
        page_views = []
        viewed_products = set()
        cart_contents = {}
        time_slots = sorted([0] + [random.randint(1, session_duration - 1) for _ in range(random.randint(3, 15))] + [session_duration])

        for i in range(len(time_slots) - 1):
            view_duration = time_slots[i + 1] - time_slots[i]
            page_type = determine_page_type(i, page_views)
            product, category = get_page_content(page_type, products, categories)

            if page_type == "product_detail" and product:
                product_id = product["product_id"]
                viewed_products.add(product_id)
                if random.random() < 0.3:
                    if product_id not in cart_contents:
                        cart_contents[product_id] = {"quantity": 0, "price": product["base_price"]}
                    max_possible = min(3, inventory.get_product(product_id)["current_stock"] - cart_contents[product_id]["quantity"])
                    if max_possible > 0:
                        add_qty = random.randint(1, max_possible)
                        cart_contents[product_id]["quantity"] += add_qty

            page_views.append({
                "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": view_duration
            })

        converted = False
        if cart_contents and any(p["page_type"] in ["checkout", "confirmation"] for p in page_views):
            converted = random.random() < 0.7

        session_geo = user["geo_data"].copy()
        session_geo["ip_address"] = fake.ipv4()

        session = {
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])
            },
            "viewed_products": list(viewed_products),
            "page_views": page_views,
            "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
            "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])
        }
        session_batch.append(session)
        session_counter += 1

        # Write session batch
        if len(session_batch) >= SESSION_BATCH_SIZE or session_counter >= NUM_SESSIONS:
            chunk_index = session_counter // SESSION_BATCH_SIZE
            session_file = data_dir / f"sessions_{chunk_index}.json"
            save_json_file(session_file, session_batch)
            session_batch = []
            gc.collect()
            log_memory_usage()

        if iteration % 10000 == 0:
            logger.info(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions (iteration {iteration:,})")
            log_memory_usage()

    # Save remaining sessions
    if session_batch:
        chunk_index = session_counter // SESSION_BATCH_SIZE
        session_file = data_dir / f"sessions_{chunk_index}.json"
        save_json_file(session_file, session_batch)
        session_batch = []
        gc.collect()
except Exception as e:
    logger.error(f"Session generation or saving failed: {e}")
    sys.exit(1)

# --- Transaction Generation ---
try:
    transaction_counter = 0
    transaction_batch = []
    iteration = 0

    logger.info("Generating transactions...")

    # Initialize transactions file
    transactions_file = data_dir / "transactions.json"
    try:
        with open(transactions_file, "w") as f:
            f.write("[]")
        logger.info(f"Initialized {transactions_file}")
    except Exception as e:
        logger.error(f"Failed to initialize {transactions_file}: {e}")
        sys.exit(1)

    # Load sessions for transaction generation
    session_files = sorted(data_dir.glob("sessions_*.json"))
    if not session_files:
        logger.error("No session files found. Cannot generate transactions.")
        sys.exit(1)

    for session_file in session_files:
        try:
            with open(session_file, "r") as f:
                sessions = json.load(f)
            for session in sessions:
                if transaction_counter >= NUM_TRANSACTIONS:
                    break
                if session["conversion_status"] == "converted":
                    cart_contents = session.get("cart_contents", {})
                    if cart_contents:
                        transaction_items = []
                        valid = True
                        for prod_id, details in cart_contents.items():
                            quantity = details["quantity"]
                            if quantity > 0 and inventory.update_stock(prod_id, quantity):
                                transaction_items.append({
                                    "product_id": prod_id,
                                    "quantity": quantity,
                                    "unit_price": details["price"],
                                    "subtotal": round(quantity * details["price"], 2)
                                })
                            else:
                                valid = False
                                break

                        if valid and transaction_items:
                            subtotal = sum(item["subtotal"] for item in transaction_items)
                            discount = 0
                            if random.random() < 0.2:
                                discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                                discount = round(subtotal * discount_rate, 2)
                            total = round(subtotal - discount, 2)

                            transaction = {
                                "transaction_id": generate_transaction_id(),
                                "session_id": session["session_id"],
                                "user_id": session["user_id"],
                                "timestamp": session["end_time"],
                                "items": transaction_items,
                                "subtotal": subtotal,
                                "discount": discount,
                                "total": total,
                                "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                                "status": "completed"
                            }
                            transaction_batch.append(transaction)
                            transaction_counter += 1

                    # Write transaction batch
                    if len(transaction_batch) >= TRANSACTION_BATCH_SIZE or transaction_counter >= NUM_TRANSACTIONS:
                        try:
                            with open(transactions_file, "r") as f:
                                existing_transactions = json.load(f)
                            existing_transactions.extend(transaction_batch)
                            save_json_file(transactions_file, existing_transactions)
                            transaction_batch = []
                            gc.collect()
                            log_memory_usage()
                        except Exception as e:
                            logger.error(f"Error saving transaction batch: {e}")
                            raise
        except Exception as e:
            logger.error(f"Error processing session file {session_file}: {e}")
            raise

        if iteration % 10000 == 0:
            logger.info(f"Progress: {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")
            log_memory_usage()

    # Generate additional transactions if needed
    while transaction_counter < NUM_TRANSACTIONS and iteration < MAX_ITERATIONS:
        iteration += 1
        prob = 0.5 if transaction_counter > NUM_TRANSACTIONS * 0.95 else 0.2
        if random.random() < prob:
            user = random.choice(users)
            products_in_txn = random.sample(products, k=min(3, len(products)))
            transaction_items = []
            for product in products_in_txn:
                if product["is_active"]:
                    quantity = random.randint(1, 3)
                    if inventory.update_stock(product["product_id"], quantity):
                        transaction_items.append({
                            "product_id": product["product_id"],
                            "quantity": quantity,
                            "unit_price": product["base_price"],
                            "subtotal": round(quantity * product["base_price"], 2)
                        })

            if transaction_items:
                subtotal = sum(item["subtotal"] for item in transaction_items)
                discount = 0
                if random.random() < 0.2:
                    discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                    discount = round(subtotal * discount_rate, 2)
                total = round(subtotal - discount, 2)

                transaction = {
                    "transaction_id": generate_transaction_id(),
                    "session_id": None,
                    "user_id": user["user_id"],
                    "timestamp": fake.date_time_between(
                        start_date=f"-{TIMESPAN_DAYS}d",
                        end_date="now"
                    ).isoformat(),
                    "items": transaction_items,
                    "subtotal": subtotal,
                    "discount": discount,
                    "total": total,
                    "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                    "status": random.choice(["completed", "processing", "shipped", "delivered"])
                }
                transaction_batch.append(transaction)
                transaction_counter += 1

            # Write transaction batch
            if len(transaction_batch) >= TRANSACTION_BATCH_SIZE or (transaction_counter >= NUM_TRANSACTIONS and len(transaction_batch) > 0):
                try:
                    with open(transactions_file, "r") as f:
                        existing_transactions = json.load(f)
                    existing_transactions.extend(transaction_batch)
                    save_json_file(transactions_file, existing_transactions)
                    transaction_batch = []
                    gc.collect()
                    log_memory_usage()
                except Exception as e:
                    logger.error(f"Error saving additional transaction batch: {e}")
                    raise

        if iteration % 10000 == 0:
            logger.info(f"Progress: {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")
            log_memory_usage()

    # Save remaining transactions
    if transaction_batch:
        try:
            with open(transactions_file, "r") as f:
                existing_transactions = json.load(f)
            existing_transactions.extend(transaction_batch)
            save_json_file(transactions_file, existing_transactions)
            transaction_batch = []
            gc.collect()
        except Exception as e:
            logger.error(f"Error saving final transaction batch: {e}")
            raise
except Exception as e:
    logger.error(f"Transaction generation or saving failed: {e}")
    sys.exit(1)

logger.info(f"""
Dataset generation complete!
- Sessions: {session_counter:,} (target: {NUM_SESSIONS:,})
- Transactions: {transaction_counter:,} (target: {NUM_TRANSACTIONS:,})
- Remaining products: {sum(p['current_stock'] for p in inventory.products.values()):,}
""")