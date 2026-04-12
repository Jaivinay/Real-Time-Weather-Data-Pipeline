#!/usr/bin/env python  # Specifies the Python interpreter to use when running the script
# -*- coding: utf-8 -*-  # Declares UTF-8 encoding for compatibility with special characters

"""
Spark Streaming Processor for Real-Time Weather Data
----------------------------------------------------
Consumes real-time weather data from MinIO (S3A), aggregates metrics over
sliding windows, and writes the processed results back to MinIO.
"""  # Multi-line docstring describing the purpose of this script

import logging  # Provides functionality for application-level logging
import sys  # Gives access to system-level functions (used here for log stream output)
import traceback  # Used for printing detailed exception tracebacks
from pyspark.sql import SparkSession, functions as F  # Imports Spark session creation and DataFrame functions
from pyspark.sql.types import (  # Imports PySpark data type classes for schema definition
    StructType, StructField, StringType, DoubleType, TimestampType  # Defines schema field types
)

# ------------------ Logging ------------------
logging.basicConfig(  # Configure logging behavior
    level=logging.INFO,  # Set global logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define log message format
    handlers=[logging.StreamHandler(sys.stdout)]  # Send log output to standard console
)
logger = logging.getLogger(__name__)  # Create a logger object for this module

# ------------------ MinIO Configuration ------------------
MINIO_ACCESS_KEY = "minioadmin"  # Access key for MinIO authentication
MINIO_SECRET_KEY = "minioadmin"  # Secret key for MinIO authentication
MINIO_BUCKET = "weather-data"  # Name of the MinIO bucket storing data
MINIO_ENDPOINT = "http://minio:9000"  # URL of MinIO service endpoint

# ============================================================
#                      Spark Setup
# ============================================================
def create_spark_session():  # Function that creates and configures a Spark session
    """Create and configure Spark session."""  # Function docstring describing its purpose
    logger.info("🚀 Initializing Spark session for streaming...")  # Log that Spark initialization has started

    spark = (  # Start SparkSession builder configuration
        SparkSession.builder  # Access Spark builder pattern
        .appName("RealtimeWeatherStreamingProcessor")  # Name the Spark application
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")  # Include Hadoop AWS libraries for S3 access
        .config("spark.streaming.stopGracefullyOnShutdown", "true")  # Ensures Spark stops gracefully on termination
        .config("spark.sql.shuffle.partitions", "2")  # Sets number of partitions used in shuffle operations (optimizes performance)
        .getOrCreate()  # Create or retrieve an existing Spark session
    )

    hconf = spark.sparkContext._jsc.hadoopConfiguration()  # Access underlying Hadoop configuration through Spark context
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)  # Set S3A access key for MinIO
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)  # Set S3A secret key for MinIO
    hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)  # Define MinIO endpoint for S3A client
    hconf.set("fs.s3a.path.style.access", "true")  # Enable path-style access (required for MinIO)
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Specify S3A as the file system implementation
    hconf.set("fs.s3a.connection.ssl.enabled", "false")  # Disable SSL since MinIO runs over HTTP
    hconf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")  # Use simple AWS credentials provider for static keys

    spark.sparkContext.setLogLevel("WARN")  # Reduce verbosity of Spark logs
    logger.info("✅ Spark session ready for structured streaming.")  # Log successful initialization
    return spark  # Return configured Spark session object

# ============================================================
#                      Define Schema
# ============================================================
def define_schema():  # Function defining the expected schema of incoming weather data
    """Schema for incoming real-time weather CSV files."""  # Docstring explaining purpose
    return StructType([  # Return a StructType object defining DataFrame structure
        StructField("city", StringType(), True),  # Column for city name
        StructField("temperature", DoubleType(), True),  # Temperature (°C)
        StructField("humidity", DoubleType(), True),  # Humidity percentage
        StructField("pressure", DoubleType(), True),  # Atmospheric pressure
        StructField("weather", StringType(), True),  # Weather condition description
        StructField("wind_speed", DoubleType(), True),  # Wind speed in m/s
        StructField("timestamp", StringType(), True),  # Timestamp of record as string
        StructField("run_id", StringType(), True),  # Identifier for each producer run
    ])

# ============================================================
#                Read Real-Time Stream from MinIO
# ============================================================
def read_stream_from_minio(spark):  # Function to read weather stream data from MinIO
    """Read streaming weather data from MinIO raw folder."""  # Docstring explaining function
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"  # Define input path for raw streaming data
    logger.info(f"📥 Reading real-time weather stream from: {s3_path}")  # Log which S3 path is being read

    schema = define_schema()  # Get predefined schema for weather data

    try:  # Attempt to read the streaming data
        streaming_df = (  # Create streaming DataFrame from S3 source
            spark.readStream  # Access Spark Structured Streaming reader
            .schema(schema)  # Apply schema for consistent parsing
            .option("header", "true")  # CSV files contain headers
            .csv(s3_path)  # Define MinIO S3 path to monitor for new files
        )

        streaming_df = (  # Apply transformations to ensure correct data types
            streaming_df
            .withColumn("event_time", F.to_timestamp("timestamp"))  # Convert timestamp string to actual timestamp
            .withColumn("temperature", F.col("temperature").cast(DoubleType()))  # Ensure temperature is double
            .withColumn("humidity", F.col("humidity").cast(DoubleType()))  # Ensure humidity is double
            .withColumn("pressure", F.col("pressure").cast(DoubleType()))  # Ensure pressure is double
            .withColumn("wind_speed", F.col("wind_speed").cast(DoubleType()))  # Ensure wind speed is double
        )

        logger.info("✅ Streaming source initialized successfully.")  # Log success
        return streaming_df  # Return the streaming DataFrame
    except Exception as e:  # Handle exceptions if reading fails
        logger.error(f"❌ Error reading stream from MinIO: {e}")  # Log error message
        logger.error(traceback.format_exc())  # Print full stack trace for debugging
        return None  # Return None if stream initialization fails

# ============================================================
#              Process Streaming Weather Data
# ============================================================
def process_streaming_data(streaming_df):  # Function to process streaming weather data
    """Compute rolling weather metrics."""  # Docstring describing functionality
    if streaming_df is None:  # Check if no streaming data is available
        logger.warning("⚠️ No streaming data to process.")  # Log warning
        return None  # Return None to exit early

    try:  # Attempt to process stream
        logger.info("⚙️ Applying streaming aggregations (10-min window)...")  # Log aggregation step

        streaming_df = streaming_df.withWatermark("event_time", "10 minutes")  # Add watermark for late data handling (10 min threshold)

        processed_df = (  # Define aggregated streaming transformations
            streaming_df
            .groupBy(  # Group data for windowed aggregation
                F.window("event_time", "10 minutes", "5 minutes"),  # Define 10-min tumbling window sliding every 5 minutes
                F.col("city")  # Group by city
            )
            .agg(  # Aggregate metrics for each group
                F.avg("temperature").alias("avg_temp"),  # Average temperature
                F.max("temperature").alias("max_temp"),  # Maximum temperature
                F.min("temperature").alias("min_temp"),  # Minimum temperature
                F.avg("humidity").alias("avg_humidity"),  # Average humidity
                F.avg("pressure").alias("avg_pressure"),  # Average pressure
                F.max("wind_speed").alias("max_wind_speed"),  # Maximum wind speed
                F.count("*").alias("record_count"),  # Count number of records per window
            )
            .withColumn("window_start", F.col("window.start"))  # Extract window start time
            .withColumn("window_end", F.col("window.end"))  # Extract window end time
            .drop("window")  # Drop window struct column to flatten schema
        )

        logger.info("✅ Real-time aggregations defined successfully.")  # Log successful aggregation setup
        return processed_df  # Return processed DataFrame
    except Exception as e:  # Handle any streaming computation errors
        logger.error(f"❌ Error processing streaming data: {e}")  # Log error
        logger.error(traceback.format_exc())  # Log detailed traceback
        return None  # Return None if failure occurs

# ============================================================
#               Write Processed Stream to MinIO
# ============================================================
def write_stream_to_minio(processed_df):  # Function to write processed stream back to MinIO
    """Write processed weather stream to MinIO."""  # Docstring describing purpose
    if processed_df is None:  # Check if no processed data exists
        logger.error("⚠️ No processed stream available to write.")  # Log error message
        return None  # Exit early

    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"  # Define destination path for processed data
    checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/realtime_stream/"  # Define checkpoint directory for stream recovery

    logger.info(f"💾 Writing streaming output to: {output_path}")  # Log output path

    try:  # Attempt to start streaming write
        query = (  # Create streaming write query
            processed_df.writeStream  # Access writeStream API for continuous output
            .outputMode("append")  # Append new records to output files
            .option("checkpointLocation", checkpoint_path)  # Define checkpoint for state management
            .format("parquet")  # Write output as Parquet files
            .partitionBy("city")  # Partition output data by city
            .trigger(processingTime="2 minutes")  # Trigger output every 2 minutes
            .start(output_path)  # Start writing to specified output path
        )

        logger.info("✅ Streaming write started successfully.")  # Log successful start
        return query  # Return active streaming query object
    except Exception as e:  # Handle exceptions during writing
        logger.error(f"❌ Failed to write streaming data: {e}")  # Log error message
        logger.error(traceback.format_exc())  # Print stack trace for debugging
        return None  # Return None if write failed

# ============================================================
#                        MAIN
# ============================================================
def main():  # Main entry function for the script
    logger.info("\n==========================================")  # Print header separator
    logger.info("🌦️  STARTING WEATHER STREAMING PROCESSOR")  # Print starting banner
    logger.info("==========================================\n")  # Print footer separator

    spark = create_spark_session()  # Create Spark session

    try:  # Try running the full stream pipeline
        streaming_df = read_stream_from_minio(spark)  # Read streaming data from MinIO
        processed_df = process_streaming_data(streaming_df)  # Apply transformations and aggregations

        if processed_df is not None:  # Check if processing succeeded
            query = write_stream_to_minio(processed_df)  # Write processed stream back to MinIO
            if query is not None:  # Check if query started successfully
                logger.info("✅ Weather streaming query running...")  # Log active query status
                query.awaitTermination()  # Keep the query running until stopped
        else:
            logger.warning("⚠️ No data to process in stream.")  # Log warning for no data case
    except Exception as e:  # Catch unexpected runtime errors
        logger.error(f"💥 Fatal error: {e}")  # Log error message
        logger.error(traceback.format_exc())  # Print detailed traceback
    finally:  # Always execute cleanup
        logger.info("🛑 Stopping Spark session...")  # Log session stop message
        spark.stop()  # Stop Spark session gracefully
        logger.info("✅ Spark session stopped. Streaming processor complete.")  # Confirm shutdown completion

if __name__ == "__main__":  # Execute only if script is run directly (not imported)
    main()  # Call main() to start the streaming processor
