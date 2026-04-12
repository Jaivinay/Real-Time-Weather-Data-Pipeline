"""
Batch Weather Data Consumer
---------------------------
Consumes historical weather batch messages from Kafka topic 'weather-batch',
stores them in MinIO as partitioned CSV files (by city/date),
and prepares them for Spark batch processing.
"""  # ← Docstring explaining script purpose: consumes Kafka weather data and stores to MinIO for later processing.

import json                                 # Used to parse and decode Kafka messages in JSON format.
import logging                              # For logging info, warnings, and errors throughout the script.
import os                                   # Provides access to environment variables for configuration.
from datetime import datetime               # For timestamps and folder partitioning in MinIO.
from io import StringIO, BytesIO            # In-memory text and byte buffers used when writing CSV files.

import pandas as pd                         # To transform batches of JSON messages into structured CSV data.
from confluent_kafka import Consumer        # Kafka client library for consuming messages from a topic.
from minio import Minio                     # MinIO SDK for interacting with object storage.
from minio.error import S3Error             # Specific exception type for MinIO errors.
from dotenv import load_dotenv              # Loads environment variables from a .env file.

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
load_dotenv()                               # Loads .env file contents (Kafka/MinIO credentials) into process environment.

# ------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------
logging.basicConfig(                        # Sets up basic logging format.
    level=logging.INFO,                     # Log all INFO-level and higher messages.
    format="%(asctime)s [%(levelname)s] %(message)s",  # Include timestamp, log level, and message text.
)
logger = logging.getLogger(__name__)        # Logger instance used throughout the module.

# ------------------------------------------------------------
# Kafka Configuration
# ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")  
# Kafka broker list. Defaults to localhost for local testing.

KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", "weather-batch")  
# Topic to consume from, typically populated by the producer script.

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_BATCH_ID", "weather-batch-consumer")  
# Consumer group ID ensures coordinated consumption across multiple consumers.

# ------------------------------------------------------------
# MinIO Configuration
# ------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")       # MinIO host:port endpoint.
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "<SECRET>")         # Masked access key for security.
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "<SECRET>")         # Masked secret key for security.
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")             # Target bucket name for storing CSVs.

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def create_minio_client():
    """Initialize MinIO client."""
    return Minio(                         # Create MinIO connection object.
        MINIO_ENDPOINT,                   # Use endpoint from environment variable.
        access_key=MINIO_ACCESS_KEY,      # Supply access key credential.
        secret_key=MINIO_SECRET_KEY,      # Supply secret key credential.
        secure=False                      # Disable HTTPS (common in local/Docker setups).
    )


def ensure_bucket_exists(minio_client, bucket_name):
    """Create bucket if it doesn't exist."""
    try:
        if not minio_client.bucket_exists(bucket_name):      # Check if target bucket exists.
            minio_client.make_bucket(bucket_name)            # Create it if missing.
            logger.info(f"🪣 Created bucket: {bucket_name}")  # Log creation event.
        else:
            logger.info(f"🪣 Bucket already exists: {bucket_name}")  # Log if already present.
    except S3Error as e:
        logger.error(f"❌ Failed to create bucket {bucket_name}: {e}")  # Log MinIO error message.
        raise                                           # Reraise to halt execution.

# ------------------------------------------------------------
# Main Consumer Logic
# ------------------------------------------------------------
def main():
    # Initialize MinIO client
    mc = create_minio_client()                # Create connection to MinIO service.
    ensure_bucket_exists(mc, MINIO_BUCKET)    # Verify that the bucket exists before writing.

    # Kafka Consumer setup
    conf = {                                  # Kafka consumer configuration dictionary.
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,  # Kafka broker(s) to connect to.
        "group.id": KAFKA_GROUP_ID,                  # Consumer group ID for offset coordination.
        "auto.offset.reset": "earliest",             # Start reading from beginning if no offset committed.
        "enable.auto.commit": False,                 # Disable automatic offset commit (manual commit after write).
    }

    consumer = Consumer(conf)                 # Create Kafka consumer instance with configuration.
    consumer.subscribe([KAFKA_TOPIC_BATCH])   # Subscribe to one or more Kafka topics.
    logger.info(f"🚀 Starting Weather Batch Consumer on topic '{KAFKA_TOPIC_BATCH}'")

    batch_buffer = []                         # Temporary list to store incoming Kafka messages before writing.
    FLUSH_SIZE = 100                          # Max number of messages before triggering flush.
    FLUSH_INTERVAL = 60                       # Max seconds before forced flush even if FLUSH_SIZE not reached.
    last_flush = datetime.now().timestamp()   # Track time of last flush to control interval-based writes.

    try:
        while True:                                   # Infinite loop to consume continuously.
            msg = consumer.poll(timeout=1.0)          # Poll Kafka for messages every second.

            if msg is None:                           # No new messages received (timeout).
                continue                              # Retry polling.
            if msg.error():                           # If message retrieval failed.
                logger.error(f"⚠️ Consumer error: {msg.error()}")  # Log the error and skip.
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))   # Decode binary Kafka message to JSON.
                batch_buffer.append(record)                        # Append decoded record to buffer.

                # Periodic flush or threshold flush
                now = datetime.now().timestamp()                   # Capture current time.
                if len(batch_buffer) >= FLUSH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                    df = pd.DataFrame(batch_buffer)                # Convert buffered JSON list into pandas DataFrame.

                    if df.empty:                                   # Skip writing if no valid data.
                        batch_buffer = []                          # Reset buffer.
                        continue

                    # Derive batch_date partitions for folder organization.
                    if "batch_date" in df.columns:                 # If messages already have a batch_date.
                        df["batch_date"] = pd.to_datetime(df["batch_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    else:
                        df["batch_date"] = datetime.utcnow().strftime("%Y-%m-%d")  # Default to today's date if missing.

                    # Add city column if not included in payloads.
                    if "city" not in df.columns:
                        df["city"] = "unknown"                     # Use placeholder city if missing.

                    grouped = df.groupby(["batch_date", "city"])   # Group messages by date and city.

                    for (batch_date, city), gdf in grouped:        # Process each city-date group separately.
                        city_safe = city.replace(" ", "_").replace(",", "_")  # Sanitize city name for file paths.
                        year, month, day = batch_date.split("-")   # Split date into year/month/day for partitioning.

                        # Create clean CSV in memory.
                        csv_buffer = StringIO()                    # In-memory text buffer for CSV creation.
                        gdf.to_csv(csv_buffer, index=False)        # Write DataFrame to buffer as CSV.
                        csv_bytes = csv_buffer.getvalue().encode("utf-8")  # Convert text buffer to bytes.

                        # Construct MinIO object path for partitioned folder structure.
                        object_name = (
                            f"raw/historical/year={year}/month={month}/day={day}/"
                            f"city={city_safe}/weather_{batch_date}_{city_safe}.csv"
                        )

                        try:
                            mc.put_object(                         # Upload the file to MinIO.
                                bucket_name=MINIO_BUCKET,          # Target bucket.
                                object_name=object_name,           # S3-style object key.
                                data=BytesIO(csv_bytes),           # Provide file contents as byte stream.
                                length=len(csv_bytes),             # File length in bytes.
                                content_type="text/csv",           # MIME type for CSV.
                            )
                            logger.info(
                                f"✅ Wrote {len(gdf)} records for {city_safe} → s3://{MINIO_BUCKET}/{object_name}"
                            )  # Log successful upload.
                        except S3Error as e:
                            logger.error(f"❌ Failed to write for {city_safe}: {e}")  # Log upload failure.
                            continue

                    consumer.commit()                              # Commit Kafka offsets after successful write.
                    batch_buffer = []                              # Clear buffer for next batch.
                    last_flush = now                               # Reset flush timer.

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")          # Handle malformed JSON messages.
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")   # Catch other message-related errors.
                continue                                           # Skip to next message.

    except KeyboardInterrupt:                                       # Handle user-initiated stop (Ctrl+C).
        logger.info("🛑 Consumer interrupted — flushing remaining messages...")
        if batch_buffer:                                           # If unflushed messages remain in buffer.
            df = pd.DataFrame(batch_buffer)                        # Convert remaining messages into DataFrame.
            if not df.empty:                                       # Ensure there is something to write.
                logger.info(f"🧾 Flushing {len(df)} remaining messages...")
                # Create backup file with timestamp.
                now = datetime.utcnow()
                timestamp = now.strftime("%Y%m%d_%H%M%S")
                object_name = f"raw/historical/unflushed/weather_backup_{timestamp}.csv"
                csv_buffer = StringIO()                            # Create text buffer for CSV.
                df.to_csv(csv_buffer, index=False)                 # Convert DataFrame to CSV format.
                csv_bytes = csv_buffer.getvalue().encode("utf-8")  # Convert to bytes for upload.
                mc.put_object(                                     # Upload backup file to MinIO.
                    bucket_name=MINIO_BUCKET,
                    object_name=object_name,
                    data=BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    content_type="text/csv",
                )
                logger.info(f"✅ Backup saved for {len(df)} messages at {object_name}")  # Log final flush summary.
    finally:
        consumer.close()                                          # Always close Kafka connection on exit.
        logger.info("🔒 Kafka Consumer closed.")                   # Confirm shutdown completion.

# ------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------
if __name__ == "__main__":   # Ensure code runs only when executed directly.
    main()                   # Start main consumer workflow.
