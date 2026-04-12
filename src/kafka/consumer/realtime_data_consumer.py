import json                                   # For decoding JSON strings coming from Kafka messages
import logging                                # For structured logging of events, errors, and status
import os                                     # For reading environment variables
import time                                   # For time delays, timestamps, and batch timing
from datetime import datetime                 # For generating UTC timestamps and filenames
from io import StringIO, BytesIO              # For in-memory text (StringIO) and byte (BytesIO) buffers

import pandas as pd                           # To store message batches as tabular data for writing to CSV
from confluent_kafka import Consumer          # Kafka consumer client library
from minio import Minio                       # MinIO SDK for interacting with the object store
from minio.error import S3Error               # Exception class for MinIO/S3 errors
from dotenv import load_dotenv                # Loads environment variables from a .env file

# -------------------- Load Environment --------------------
load_dotenv()                                 # Loads configuration (Kafka, MinIO, etc.) from .env file into environment

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.DEBUG,                      # Log everything at DEBUG level and above
    format="%(asctime)s [%(levelname)s] %(message)s",  # Include timestamp, severity, and message
)
logger = logging.getLogger(__name__)          # Create a logger for this module

# -------------------- Kafka Configuration --------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
# Kafka broker address, defaults to localhost if not defined in environment

KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME", "weather-realtime")
# Kafka topic from which weather messages will be consumed

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_REALTIME_ID", "weather-realtime-consumer")
# Kafka consumer group ID (used for offset tracking and parallelism)

# -------------------- MinIO Configuration --------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")       # MinIO service host:port
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "<SECRET>")         # Access key (masked)
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "<SECRET>")         # Secret key (masked)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")             # Target bucket to store ingested CSVs

# -------------------- Helper Functions --------------------
def create_minio_client():
    """Initialize and return a MinIO client instance."""
    return Minio(
        MINIO_ENDPOINT,                   # Host and port for MinIO server
        access_key=MINIO_ACCESS_KEY,      # Authentication access key
        secret_key=MINIO_SECRET_KEY,      # Authentication secret key
        secure=False                      # Disable HTTPS for local development
    )

def ensure_bucket_exists(mc, bucket_name):
    """Create bucket if it doesn't already exist."""
    try:
        if not mc.bucket_exists(bucket_name):    # Check if target bucket is present
            mc.make_bucket(bucket_name)          # Create it if missing
            logger.info(f"🪣 Created bucket: {bucket_name}")  # Log bucket creation
        else:
            logger.info(f"🪣 Bucket '{bucket_name}' already exists")  # Log if bucket already exists
    except S3Error as e:
        logger.error(f"❌ Error checking/creating bucket {bucket_name}: {e}")  # Log MinIO error
        raise

# -------------------- Main Function --------------------
def main():
    # Initialize MinIO client
    mc = create_minio_client()                   # Connect to MinIO using credentials
    ensure_bucket_exists(mc, MINIO_BUCKET)       # Verify that the bucket exists (create if needed)

    # Configure Kafka consumer parameters
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,  # Broker connection string
        "group.id": KAFKA_GROUP_ID,                    # Consumer group ID
        "auto.offset.reset": "earliest",               # Start from earliest message if no offset committed
        "enable.auto.commit": False,                   # Manual commit after successful batch write
    }

    consumer = Consumer(conf)                         # Instantiate Kafka Consumer with config
    consumer.subscribe([KAFKA_TOPIC_REALTIME])        # Subscribe to the specified Kafka topic
    logger.info(f"🌦️ Starting Weather Consumer on topic '{KAFKA_TOPIC_REALTIME}'")

    # Batching setup parameters
    DEFAULT_BATCH_SIZE = 50          # Number of messages before triggering a write to MinIO
    FLUSH_INTERVAL = 60              # Maximum time (seconds) between writes, even if batch incomplete
    messages = []                    # List to accumulate incoming Kafka messages
    flush_time = time.time()         # Timestamp of last batch flush

    try:
        while True:                                      # Continuous consumer loop
            msg = consumer.poll(timeout=1.0)             # Poll Kafka for a message every second

            if msg is None:                              # No message received (timeout)
                continue                                 # Retry polling
            if msg.error():                              # If message object has an error
                logger.error(f"⚠️ Consumer error: {msg.error()}")  # Log the Kafka error
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))  # Decode and parse JSON payload
                messages.append(value)                            # Add record to current batch

                # Log progress every 10 messages
                if len(messages) % 10 == 0:
                    logger.info(f"💡 {len(messages)} weather records collected in current batch")

                # Check flush conditions: either batch is full OR time interval reached
                current_time = time.time()
                if (len(messages) >= DEFAULT_BATCH_SIZE) or (
                    current_time - flush_time >= FLUSH_INTERVAL and len(messages) > 0
                ):
                    df = pd.DataFrame(messages)                   # Convert batch list into DataFrame
                    now = datetime.utcnow()                       # Capture current UTC time
                    timestamp = now.strftime("%Y%m%d_%H%M%S")     # Generate timestamp for filename

                    # Organize files in partitioned S3-style folder structure (year/month/day/hour)
                    object_name = (
                        f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                        f"hour={now.hour:02d}/weather_data_{timestamp}.csv"
                    )

                    csv_buffer = StringIO()                       # Create in-memory text buffer
                    df.to_csv(csv_buffer, index=False)             # Write DataFrame as CSV into buffer
                    csv_bytes = csv_buffer.getvalue().encode("utf-8")  # Encode CSV text into bytes

                    try:
                        mc.put_object(                            # Upload the CSV to MinIO
                            bucket_name=MINIO_BUCKET,             # Target bucket
                            object_name=object_name,              # S3 key (file path)
                            data=BytesIO(csv_bytes),              # Provide data as byte stream
                            length=len(csv_bytes),                # Specify content length
                            content_type="text/csv",              # MIME type
                        )
                        logger.info(
                            f"✅ Wrote {len(messages)} weather records → s3://{MINIO_BUCKET}/{object_name}"
                        )
                    except S3Error as e:
                        logger.error(f"❌ Failed to write batch to MinIO: {e}")  # Log upload failure
                        raise

                    messages = []                                 # Clear in-memory batch buffer
                    flush_time = time.time()                      # Reset flush timer
                    consumer.commit()                             # Commit Kafka offsets manually

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")         # Handle malformed JSON message
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")  # Handle other message errors

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        logger.info("🛑 Stopping consumer gracefully...")

        if messages:                                              # If unflushed messages remain
            logger.info("⏳ Flushing remaining messages before exit...")
            df = pd.DataFrame(messages)                            # Convert remaining messages to DataFrame
            now = datetime.utcnow()
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            # Use a slightly different file suffix for final flush
            object_name = (
                f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"hour={now.hour:02d}/weather_data_{timestamp}_final.csv"
            )
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode("utf-8")
            mc.put_object(                                        # Upload final records
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=BytesIO(csv_bytes),
                length=len(csv_bytes),
                content_type="text/csv",
            )
            logger.info(f"✅ Final flush complete ({len(df)} records)")  # Log final upload confirmation

    finally:
        consumer.close()                                          # Close Kafka consumer connection
        logger.info("🔒 Kafka Consumer closed.")                   # Confirm consumer shutdown

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    main()                                                        # Execute main() only when script run directly
