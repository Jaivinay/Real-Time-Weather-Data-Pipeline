import json
import logging
import os
import time
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# -------------------- Load Environment --------------------
load_dotenv()

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------- Kafka Configuration --------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME", "weather-realtime")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_REALTIME_ID", "weather-realtime-consumer")

# -------------------- MinIO Configuration --------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")

# -------------------- Helper Functions --------------------
def create_minio_client():
    """Initialize MinIO Client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(mc, bucket_name):
    """Ensure MinIO bucket exists."""
    try:
        if not mc.bucket_exists(bucket_name):
            mc.make_bucket(bucket_name)
            logger.info(f"🪣 Created bucket: {bucket_name}")
        else:
            logger.info(f"🪣 Bucket '{bucket_name}' already exists")
    except S3Error as e:
        logger.error(f"❌ Error checking/creating bucket {bucket_name}: {e}")
        raise

# -------------------- Main Function --------------------
def main():
    # Initialize MinIO
    mc = create_minio_client()
    ensure_bucket_exists(mc, MINIO_BUCKET)

    # Kafka Consumer Configuration
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_REALTIME])

    logger.info(f"🌦️ Starting Weather Consumer on topic '{KAFKA_TOPIC_REALTIME}'")

    # Batching setup
    DEFAULT_BATCH_SIZE = 50          # number of messages per batch
    FLUSH_INTERVAL = 60              # seconds
    messages = []
    flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"⚠️ Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)

                # Log progress
                if len(messages) % 10 == 0:
                    logger.info(f"💡 {len(messages)} weather records collected in current batch")

                # Check flush conditions
                current_time = time.time()
                if (len(messages) >= DEFAULT_BATCH_SIZE) or (
                    current_time - flush_time >= FLUSH_INTERVAL and len(messages) > 0
                ):
                    df = pd.DataFrame(messages)
                    now = datetime.utcnow()
                    timestamp = now.strftime("%Y%m%d_%H%M%S")

                    # Partitioned folder structure
                    object_name = (
                        f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                        f"hour={now.hour:02d}/weather_data_{timestamp}.csv"
                    )

                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_bytes = csv_buffer.getvalue().encode("utf-8")

                    try:
                        mc.put_object(
                            bucket_name=MINIO_BUCKET,
                            object_name=object_name,
                            data=BytesIO(csv_bytes),
                            length=len(csv_bytes),
                            content_type="text/csv",
                        )
                        logger.info(
                            f"✅ Wrote {len(messages)} weather records → s3://{MINIO_BUCKET}/{object_name}"
                        )
                    except S3Error as e:
                        logger.error(f"❌ Failed to write batch to MinIO: {e}")
                        raise

                    messages = []
                    flush_time = time.time()

                    consumer.commit()

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("🛑 Stopping consumer gracefully...")
        if messages:
            logger.info("⏳ Flushing remaining messages before exit...")
            df = pd.DataFrame(messages)
            now = datetime.utcnow()
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            object_name = (
                f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"hour={now.hour:02d}/weather_data_{timestamp}_final.csv"
            )
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
            logger.info(f"✅ Final flush complete ({len(df)} records)")

    finally:
        consumer.close()
        logger.info("🔒 Kafka Consumer closed.")

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    main()
