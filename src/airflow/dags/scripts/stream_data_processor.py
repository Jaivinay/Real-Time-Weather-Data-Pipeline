#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Streaming Processor for Real-Time Weather Data
----------------------------------------------------
Consumes real-time weather data from MinIO (S3A), aggregates metrics over
sliding windows, and writes the processed results back to MinIO.
"""

import logging
import sys
import traceback
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# ------------------ Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ------------------ MinIO Configuration ------------------
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "weather-data"
MINIO_ENDPOINT = "minio:9000"

# ============================================================
#                      Spark Setup
# ============================================================
def create_spark_session():
    """Create and configure Spark session."""
    logger.info("🚀 Initializing Spark session for streaming...")

    spark = (
        SparkSession.builder
        .appName("RealtimeWeatherStreamingProcessor")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session ready for structured streaming.")
    return spark

# ============================================================
#                      Define Schema
# ============================================================
def define_schema():
    """Schema for incoming real-time weather CSV files."""
    return StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("weather", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("run_id", StringType(), True),
    ])

# ============================================================
#                Read Real-Time Stream from MinIO
# ============================================================
def read_stream_from_minio(spark):
    """Read streaming weather data from MinIO raw folder."""
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"
    logger.info(f"📥 Reading real-time weather stream from: {s3_path}")

    schema = define_schema()

    try:
        streaming_df = (
            spark.readStream
            .schema(schema)
            .option("header", "true")
            .csv(s3_path)
        )

        streaming_df = (
            streaming_df
            .withColumn("event_time", F.to_timestamp("timestamp"))
            .withColumn("temperature", F.col("temperature").cast(DoubleType()))
            .withColumn("humidity", F.col("humidity").cast(DoubleType()))
            .withColumn("pressure", F.col("pressure").cast(DoubleType()))
            .withColumn("wind_speed", F.col("wind_speed").cast(DoubleType()))
        )

        logger.info("✅ Streaming source initialized successfully.")
        return streaming_df
    except Exception as e:
        logger.error(f"❌ Error reading stream from MinIO: {e}")
        logger.error(traceback.format_exc())
        return None

# ============================================================s
#              Process Streaming Weather Data
# ============================================================
def process_streaming_data(streaming_df):
    """Compute rolling weather metrics."""
    if streaming_df is None:
        logger.warning("⚠️ No streaming data to process.")
        return None

    try:
        logger.info("⚙️ Applying streaming aggregations (10-min window)...")

        streaming_df = streaming_df.withWatermark("event_time", "10 minutes")

        processed_df = (
            streaming_df
            .groupBy(
                F.window("event_time", "10 minutes", "5 minutes"),
                F.col("city")
            )
            .agg(
                F.avg("temperature").alias("avg_temp"),
                F.max("temperature").alias("max_temp"),
                F.min("temperature").alias("min_temp"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("pressure").alias("avg_pressure"),
                F.max("wind_speed").alias("max_wind_speed"),
                F.count("*").alias("record_count"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )

        logger.info("✅ Real-time aggregations defined successfully.")
        return processed_df
    except Exception as e:
        logger.error(f"❌ Error processing streaming data: {e}")
        logger.error(traceback.format_exc())
        return None

# ============================================================
#               Write Processed Stream to MinIO
# ============================================================
def write_stream_to_minio(processed_df):
    """Write processed weather stream to MinIO."""
    if processed_df is None:
        logger.error("⚠️ No processed stream available to write.")
        return None

    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
    checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/realtime_stream/"

    logger.info(f"💾 Writing streaming output to: {output_path}")

    try:
        query = (
            processed_df.writeStream
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .format("parquet")
            .partitionBy("city")
            .trigger(processingTime="2 minutes")
            .start(output_path)
        )

        logger.info("✅ Streaming write started successfully.")
        return query
    except Exception as e:
        logger.error(f"❌ Failed to write streaming data: {e}")
        logger.error(traceback.format_exc())
        return None

# ============================================================
#                        MAIN
# ============================================================
def main():
    logger.info("\n==========================================")
    logger.info("🌦️  STARTING WEATHER STREAMING PROCESSOR")
    logger.info("==========================================\n")

    spark = create_spark_session()

    try:
        streaming_df = read_stream_from_minio(spark)
        processed_df = process_streaming_data(streaming_df)

        if processed_df is not None:
            query = write_stream_to_minio(processed_df)
            if query is not None:
                logger.info("✅ Weather streaming query running...")
                query.awaitTermination()
        else:
            logger.warning("⚠️ No data to process in stream.")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("🛑 Stopping Spark session...")
        spark.stop()
        logger.info("✅ Spark session stopped. Streaming processor complete.")

if __name__ == "__main__":
    main()


