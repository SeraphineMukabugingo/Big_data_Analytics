import resource
import logging
import gc
import psutil
import happybase
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from thriftpy2.transport import TTransportException
import time
from typing import Dict, Tuple, DefaultDict, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("hbase_load.log")
    ]
)
logger = logging.getLogger(__name__)

# Constants
MEMORY_LIMIT = 14 * 1024**3  # 14GB
BATCH_SIZE = 500
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
HBASE_HOST = 'localhost'
HBASE_TIMEOUT = 60000  # 60 seconds
DATA_DIR = Path("/home/seraphine/Big_Data_Final_project/ecommerce_project/data_json")

# Type aliases
ProductMetrics = Dict[str, int]
DateStr = str
ProductID = str
MetricsKey = Tuple[ProductID, DateStr]

def set_memory_limit() -> None:
    """Set memory limit for the process"""
    try:
        resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
        logger.info(f"Memory limit set to {MEMORY_LIMIT/1024**3:.1f}GB")
    except (ValueError, resource.error) as e:
        logger.warning(f"Memory limit not set: {e}")

def get_hbase_connection(retries: int = MAX_RETRIES) -> happybase.Connection:
    """Establish HBase connection with retry logic"""
    for attempt in range(retries):
        try:
            conn = happybase.Connection(
                HBASE_HOST,
                timeout=HBASE_TIMEOUT,
                autoconnect=False
            )
            conn.open()
            logger.info("HBase connection established")
            return conn
        except TTransportException as e:
            if attempt == retries - 1:
                logger.error("Max connection retries reached")
                raise
            delay = RETRY_DELAY * (attempt + 1)
            logger.warning(f"Connection failed (attempt {attempt+1}), retrying in {delay}s...")
            time.sleep(delay)
    raise ConnectionError("Failed to establish HBase connection")

def create_tables(connection: happybase.Connection) -> None:
    """Create HBase tables if they don't exist"""
    tables = {
        'UserSessions': {
            'session_info': {'max_versions': 3},
            'geo_device': {},
            'activity': {}
        },
        'ProductMetrics': {
            'metrics': {}
        }
    }

    existing_tables = set(connection.tables())
    
    for table_name, families in tables.items():
        if table_name not in existing_tables:
            try:
                connection.create_table(table_name, families)
                logger.info(f"Created table {table_name}")
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                raise

def get_session_files() -> List[Path]:
    """Get sorted session files from data directory"""
    session_files = sorted(
        [f for i in range(1, 21) for f in [DATA_DIR/f"sessions_{i}.json"] if f.exists()],
        key=lambda x: int(x.stem.split('_')[1])
    )
    
    if not session_files:
        logger.error("No session files found")
        raise FileNotFoundError("No session files found in data directory")
    
    logger.info(f"Found {len(session_files)} session files to process")
    return session_files

def process_session_batch(
    batch: happybase.Batch,
    batch_metrics: DefaultDict[MetricsKey, ProductMetrics],
    sessions: List[dict]
) -> None:
    """Process a batch of sessions and update metrics"""
    for session in sessions:
        try:
            # Normalize timestamp for row key
            ts = session['start_time'].replace(':', '').replace('-', '')
            row_key = f"{session['user_id']}#{ts}"
            
            # Prepare session data
            batch.put(row_key, {
                b'session_info:session_id': session['session_id'].encode(),
                b'session_info:start_time': session['start_time'].encode(),
                b'activity:viewed_products': json.dumps(session['viewed_products']).encode()
            })

            # Update metrics
            date = datetime.fromisoformat(session['start_time']).strftime('%Y%m%d')
            for pid in session['viewed_products']:
                batch_metrics[(pid, date)]['views'] += 1
            
            for pid, item in session.get('cart_contents', {}).items():
                batch_metrics[(pid, date)]['cart_additions'] += item['quantity']
                if session['conversion_status'] == 'converted':
                    batch_metrics[(pid, date)]['purchases'] += item['quantity']

        except Exception as e:
            logger.warning(f"Skipping session {session.get('session_id')}: {e}")

def load_product_metrics(
    metrics_table: happybase.Table,
    product_metrics: Dict[MetricsKey, ProductMetrics]
) -> None:
    """Load product metrics into HBase with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            metrics_batch = metrics_table.batch()
            for (pid, date), counts in product_metrics.items():
                metrics_batch.put(
                    f"{pid}#{date}",
                    {f'metrics:{k}'.encode(): str(v).encode() for k,v in counts.items()}
                )
            metrics_batch.send()
            logger.info(f"Loaded {len(product_metrics)} product metrics")
            return
        except TTransportException:
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning(f"Metrics batch failed (attempt {attempt+1}), retrying...")
            time.sleep(RETRY_DELAY * (attempt + 1))

def main() -> None:
    """Main data loading process"""
    set_memory_limit()
    
    try:
        # Initialize HBase connection
        connection = get_hbase_connection()
        
        # Create tables if needed
        create_tables(connection)
        
        # Get session files
        session_files = get_session_files()
        
        # Initialize tables and metrics
        sessions_table = connection.table('UserSessions')
        metrics_table = connection.table('ProductMetrics')
        product_metrics: DefaultDict[MetricsKey, ProductMetrics] = defaultdict(
            lambda: {'views': 0, 'cart_additions': 0, 'purchases': 0}
        )
        total_sessions = 0

        # Process each file
        for file in session_files:
            logger.info(f"Processing {file.name}")
            
            with open(file) as f:
                sessions = json.load(f)

            # Process in batches
            for i in range(0, len(sessions), BATCH_SIZE):
                batch_sessions = sessions[i:i+BATCH_SIZE]
                
                try:
                    batch = sessions_table.batch()
                    batch_metrics: DefaultDict[MetricsKey, ProductMetrics] = defaultdict(
                        lambda: {'views': 0, 'cart_additions': 0, 'purchases': 0}
                    )
                    
                    process_session_batch(batch, batch_metrics, batch_sessions)
                    batch.send()
                    total_sessions += len(batch_sessions)
                    
                    # Aggregate metrics
                    for key, counts in batch_metrics.items():
                        product_metrics[key]['views'] += counts['views']
                        product_metrics[key]['cart_additions'] += counts['cart_additions']
                        product_metrics[key]['purchases'] += counts['purchases']

                    logger.info(f"Processed {total_sessions:,} sessions ({(total_sessions/len(sessions))*100:.1f}%)")
                    gc.collect()

                except (TTransportException, happybase.ConnectionError) as e:
                    logger.warning(f"Connection error: {e}, reconnecting...")
                    connection = get_hbase_connection()
                    sessions_table = connection.table('UserSessions')
                    continue

        # Load product metrics
        load_product_metrics(metrics_table, product_metrics)

        logger.info(f"""
        Loading complete!
        - Sessions loaded: {total_sessions:,}
        - Product metrics: {len(product_metrics):,}
        """)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if 'connection' in locals():
            connection.close()
            logger.info("HBase connection closed")

if __name__ == "__main__":
    main()