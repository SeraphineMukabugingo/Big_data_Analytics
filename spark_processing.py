from pyspark.sql import SparkSession
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def revenue_by_state(spark):
    try:
        # Verify data directory exists
        data_dir = r"/home/seraphine/Big_Data_Final_project/ecommerce_project/data_json"
        if not os.path.exists(data_dir):
            logger.error(f"Data directory does not exist: {data_dir}")
            raise FileNotFoundError(f"Data directory does not exist: {data_dir}")

        # Read JSON files
        users_path = os.path.join(data_dir, 'users.json')
        transactions_path = os.path.join(data_dir, 'transactions.json')

        if not os.path.exists(users_path):
            logger.error(f"Users JSON file not found: {users_path}")
            raise FileNotFoundError(f"Users JSON file not found: {users_path}")
        if not os.path.exists(transactions_path):
            logger.error(f"Transactions JSON file not found: {transactions_path}")
            raise FileNotFoundError(f"Transactions JSON file not found: {transactions_path}")

        logger.info(f"Reading users from: {users_path}")
        logger.info(f"Reading transactions from: {transactions_path}")
        
        users_df = spark.read.json(users_path)
        transactions_df = spark.read.json(transactions_path)

        # Log schema for debugging
        logger.info("Users DataFrame schema:")
        users_df.printSchema()
        logger.info("Transactions DataFrame schema:")
        transactions_df.printSchema()

        # Create temporary views
        users_df.createOrReplaceTempView('users')
        transactions_df.createOrReplaceTempView('transactions')

        # Execute SQL query
        query = """
        SELECT u.geo_data.state, SUM(t.total) as total_revenue
        FROM users u
        JOIN transactions t ON u.user_id = t.user_id
        GROUP BY u.geo_data.state
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        logger.info("Executing revenue by state query")
        result = spark.sql(query)
        logger.info("Executed revenue by state query successfully")
        return result
    except Exception as e:
        logger.error(f"Error in revenue_by_state: {str(e)}")
        raise

if __name__ == "__main__":  # Fixed: Changed from "_main_" to "__main__"
    spark = None
    try:
        # Configure Spark session with explicit settings
        spark = SparkSession.builder \
            .appName("ECommerceAnalytics") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.port", "7078") \
            .config("spark.ui.port", "4050") \
            .master("local[*]") \
            .getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully")

        result = revenue_by_state(spark)
        if result:
            logger.info("Displaying top 10 states by revenue:")
            result.show()
    except Exception as e:
        logger.error(f"Failed to execute main process: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")