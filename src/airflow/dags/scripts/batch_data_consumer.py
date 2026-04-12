"""
Batch Weather Data Consumer
---------------------------
Consumes historical weather batch messages from Kafka topic 'weather-batch',
stores them in MinIO as partitioned CSV files (by city/date),
and prepares them for Spark batch processing.
"""

import json
import logging
import os
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Kafka Configuration
# ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", "weather-batch")
# KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_BATCH_ID", "weather-batch-consumer")
KAFKA_GROUP_ID = f"weather-batch-consumer-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

# ------------------------------------------------------------
# MinIO Configuration
# ------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def create_minio_client():
    """Initialize MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def ensure_bucket_exists(minio_client, bucket_name):
    """Create bucket if it doesn't exist."""
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"🪣 Created bucket: {bucket_name}")
        else:
            logger.info(f"🪣 Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"❌ Failed to create bucket {bucket_name}: {e}")
        raise


# ------------------------------------------------------------
# Main Consumer Logic
# ------------------------------------------------------------
def main():
    # Initialize MinIO client
    mc = create_minio_client()
    ensure_bucket_exists(mc, MINIO_BUCKET)

    # Kafka Consumer setup
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"🚀 Starting Weather Batch Consumer on topic '{KAFKA_TOPIC_BATCH}'")

    batch_buffer = []  # accumulate messages to reduce I/O
    FLUSH_SIZE = 30   # messages per write
    FLUSH_INTERVAL = 30  # seconds
    last_flush = datetime.now().timestamp()

    # 👇 Added quiet-time logic
    no_message_count = 0
    MAX_EMPTY_POLLS = 5  # stop after 5 consecutive empty polls (~10 seconds)

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                no_message_count += 1
                if no_message_count >= MAX_EMPTY_POLLS:
                    logger.info("⚡ No new messages received. Ending consumer.")
                    break
                continue
            no_message_count = 0  # reset counter when message received

            if msg.error():
                logger.error(f"⚠️ Consumer error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch_buffer.append(record)

                # Periodic flush or threshold flush
                now = datetime.now().timestamp()
                if len(batch_buffer) >= FLUSH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                    df = pd.DataFrame(batch_buffer)

                    if df.empty:
                        batch_buffer = []
                        continue

                    # Derive batch_date partitions
                    if "batch_date" in df.columns:
                        df["batch_date"] = pd.to_datetime(df["batch_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    else:
                        df["batch_date"] = datetime.utcnow().strftime("%Y-%m-%d")

                    # Add city name for partitioning
                    if "city" not in df.columns:
                        df["city"] = "unknown"

                    grouped = df.groupby(["batch_date", "city"])

                    for (batch_date, city), gdf in grouped:
                        city_safe = city.replace(" ", "_").replace(",", "_")
                        year, month, day = batch_date.split("-")

                        # Create clean CSV in memory
                        csv_buffer = StringIO()
                        gdf.to_csv(csv_buffer, index=False)
                        csv_bytes = csv_buffer.getvalue().encode("utf-8")

                        # Construct MinIO path
                        object_name = (
                            f"raw/historical/year={year}/month={month}/day={day}/"
                            f"city={city_safe}/weather_{batch_date}_{city_safe}.csv"
                        )

                        try:
                            mc.put_object(
                                bucket_name=MINIO_BUCKET,
                                object_name=object_name,
                                data=BytesIO(csv_bytes),
                                length=len(csv_bytes),
                                content_type="text/csv",
                            )
                            logger.info(f"✅ Wrote {len(gdf)} records for {city_safe} → s3://{MINIO_BUCKET}/{object_name}")
                        except S3Error as e:
                            logger.error(f"❌ Failed to write for {city_safe}: {e}")
                            continue

                    consumer.commit()
                    batch_buffer = []
                    last_flush = now

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("🛑 Consumer interrupted — flushing remaining messages...")
        if batch_buffer:
            df = pd.DataFrame(batch_buffer)
            if not df.empty:
                logger.info(f"🧾 Flushing {len(df)} remaining messages...")
                # Reuse the same flush logic once
                now = datetime.utcnow()
                timestamp = now.strftime("%Y%m%d_%H%M%S")
                object_name = f"raw/historical/unflushed/weather_backup_{timestamp}.csv"
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_bytes = csv_buffer.getvalue().encode("utf-8")
                mc.put_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=object_name,
                    data=BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    content_type="text/csv",
                )
                logger.info(f"✅ Backup saved for {len(df)} messages at {object_name}")
    finally:
        # 👇 Ensure any remaining records are flushed before exit
        if batch_buffer:
            df = pd.DataFrame(batch_buffer)
            if not df.empty:
                logger.info(f"💾 Final flush of {len(df)} messages before shutdown...")
                now = datetime.utcnow()
                timestamp = now.strftime("%Y%m%d_%H%M%S")
                object_name = f"raw/historical/unflushed/weather_final_{timestamp}.csv"
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_bytes = csv_buffer.getvalue().encode("utf-8")
                mc.put_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=object_name,
                    data=BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    content_type="text/csv",
                )
                logger.info(f"✅ Final backup saved at {object_name}")

        consumer.close()
        logger.info("🔒 Kafka Consumer closed cleanly after quiet period.")

# ------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------
if __name__ == "__main__":
    main()
