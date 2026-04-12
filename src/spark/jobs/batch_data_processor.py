# ===============================================================
# Weather Batch Processor using PySpark + MinIO (S3A)
# Every line is explained in detail for full understanding.
# ===============================================================

# ---- Import standard and Spark libraries ----
import os  # Provides access to environment variables and OS operations
from datetime import datetime  # Used for handling dates and timestamps

# Import PySpark classes
from pyspark.sql import SparkSession  # To create and configure the Spark session
from pyspark.sql import functions as F  # Provides SQL-style functions for DataFrame transformations

# ---- Redundant import (kept intentionally in original code) ----
import os  # Re-importing 'os' has no side effect but is harmless

# ===============================================================
# 1️⃣ Hadoop Timeout Configuration
# These override Hadoop defaults like "60s" → set numeric milliseconds (60000 ms = 60s)
# ===============================================================
os.environ["HADOOP_OPTS"] = "-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000"  # Set system-wide Hadoop options
os.environ["fs.s3a.connection.timeout"] = "60000"  # Timeout for S3A connections in milliseconds
os.environ["fs.s3a.connection.establish.timeout"] = "60000"  # Timeout for establishing S3A connections
os.environ["fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"  # Use Hadoop’s S3A implementation to access S3/MinIO

# ===============================================================
# 2️⃣ MinIO Configuration
# Defines credentials and endpoint for S3A to connect to MinIO.
# ===============================================================
MINIO_ACCESS_KEY = "minioadmin"  # Default MinIO username
MINIO_SECRET_KEY = "minioadmin"  # Default MinIO password
MINIO_BUCKET = "weather-data"  # Bucket where raw and processed data are stored
MINIO_ENDPOINT = "http://minio:9000"  # URL for MinIO server running inside Docker

# ===============================================================
# 3️⃣ Function: create_spark_session()
# Builds and returns a configured SparkSession that can access MinIO.
# ===============================================================
def create_spark_session():
    print("Initializing Spark Session with MinIO (S3A) configuration...")  # Notify user of Spark startup

    spark = (  # Create SparkSession builder chain
        SparkSession.builder  # Start Spark builder
        .appName("WeatherBatchProcessor")  # Name this Spark job (shown in UI/logs)
        .config(  # Add custom configuration key-value pairs
            "spark.jars.packages",  # Specify external JARs Spark should load
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",  # Include AWS Hadoop and SDK libraries
        )
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")  # Override S3A timeout
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")  # Override connection establish timeout
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")  # Limit max concurrent S3 connections
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")  # Retry failed requests up to 3 times
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Force S3A implementation
        .getOrCreate()  # Create the SparkSession (or reuse existing one)
    )

    hconf = spark.sparkContext._jsc.hadoopConfiguration()  # Get underlying Hadoop configuration object from Spark context
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)  # Provide MinIO access key
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)  # Provide MinIO secret key
    hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)  # Set endpoint URL for MinIO
    hconf.set("fs.s3a.path.style.access", "true")  # Enable path-style URLs (needed for MinIO)
    hconf.set("fs.s3a.connection.ssl.enabled", "false")  # Disable SSL since MinIO runs on HTTP
    hconf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")  # Use simple static credentials

    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark log verbosity to warnings only
    print("✅ Spark session initialized successfully.\n")  # Confirmation message
    return spark  # Return configured Spark session

# ===============================================================
# 4️⃣ Function: read_data_from_s3()
# Reads raw CSV weather data from MinIO for a given date.
# ===============================================================
def read_data_from_s3(spark, date=None):  # Accept Spark session and optional date
    print("Reading raw weather data from MinIO...")  # Log step

    process_date = datetime.now() if date is None else datetime.strptime(date, "%Y-%m-%d")  # Use today’s date or parse given one
    year, month, day = process_date.year, process_date.month, process_date.day  # Extract year/month/day integers

    s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/year={year}/month={month:02d}/day={day:02d}/city=*/"  # Build hierarchical S3 path
    print(f"📂 S3 path: {s3_path}")  # Show path for debugging

    try:
        df = (  # Attempt to read CSVs into Spark DataFrame
            spark.read.option("header", "true")  # First line is header
            .option("inferSchema", "true")  # Let Spark auto-detect column types
            .csv(s3_path)  # Read all CSVs matching the path
        )

        print("✅ Sample data from MinIO:")  # Print confirmation
        df.select("date", "city", "temperature", "humidity", "wind_speed", "precip_mm").show(5, truncate=False)  # Show first 5 rows for verification
        df.printSchema()  # Print inferred schema to verify column types

        return df  # Return loaded DataFrame

    except Exception as e:  # Catch any reading error (missing path, network issue, etc.)
        print(f"❌ Error reading data from S3: {str(e)}")  # Print error message
        return None  # Return None to indicate failure

# ===============================================================
# 5️⃣ Function: process_weather_data()
# Cleans and aggregates weather data (average temp, humidity, etc.)
# ===============================================================
def process_weather_data(df):  # Accept raw DataFrame
    print("\n---- Processing Historical Weather Data ----")  # Step header

    if df is None or df.count() == 0:  # Check if DataFrame is empty
        print("⚠️ No data to process.")  # Warn user
        return None  # Exit function

    try:
        record_count = df.count()  # Count total number of rows
        print(f"Total records: {record_count}")  # Log record count

        df = (  # Convert data types for numeric operations
            df.withColumn("date", F.to_date("date"))  # Convert date string to DateType
              .withColumn("temperature", F.col("temperature").cast("double"))  # Cast temperature to double
              .withColumn("humidity", F.col("humidity").cast("double"))  # Cast humidity to double
              .withColumn("wind_speed", F.col("wind_speed").cast("double"))  # Cast wind speed to double
              .withColumn("precip_mm", F.col("precip_mm").cast("double"))  # Cast precipitation to double
        )

        agg_df = (  # Perform aggregations per city per date
            df.groupBy("city", "date")  # Group by city and date
            .agg(  # Apply aggregation functions
                F.avg("temperature").alias("avg_temp"),  # Compute average temperature
                F.max("temperature").alias("max_temp"),  # Compute max temperature
                F.min("temperature").alias("min_temp"),  # Compute min temperature
                F.avg("humidity").alias("avg_humidity"),  # Compute average humidity
                F.max("wind_speed").alias("max_wind_speed"),  # Compute maximum wind speed
                F.sum("precip_mm").alias("total_precip_mm"),  # Sum precipitation over day
            )
            .orderBy("city", "date")  # Sort results by city/date
        )

        print("📊 Sample of processed daily metrics:")  # Show preview
        agg_df.show(10, truncate=False)  # Display top 10 aggregated rows
        return agg_df  # Return processed DataFrame

    except Exception as e:  # Catch Spark processing errors
        print(f"❌ Error processing weather data: {str(e)}")  # Display error
        return None  # Return None on failure

# ===============================================================
# 6️⃣ Function: write_to_s3()
# Writes processed DataFrame to MinIO as Parquet files.
# ===============================================================
def write_to_s3(df, date=None):  # Accept DataFrame and optional date
    print("\n------ Writing processed weather data to MinIO ------")  # Log start

    if df is None:  # Handle missing DataFrame
        print("⚠️ No data to write.")  # Warn user
        return None  # Exit early

    processed_date = datetime.now().strftime("%Y-%m-%d") if date is None else date  # Use today’s date or provided date
    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/date={processed_date}/"  # Build S3 output path
    print(f"💾 Output path: {output_path}")  # Print output location

    try:
        (  # Write DataFrame to S3
            df.write.partitionBy("city")  # Create separate folder per city
            .mode("overwrite")  # Replace existing data for same date
            .parquet(output_path)  # Save as efficient Parquet format
        )
        print(f"✅ Data written to MinIO at: {output_path}")  # Confirm success

    except Exception as e:  # Catch write errors (network, permissions, etc.)
        print(f"❌ Error writing to MinIO: {str(e)}")  # Print error message

# ===============================================================
# 7️⃣ Function: main()
# Main driver that orchestrates reading, processing, and writing.
# ===============================================================
def main():
    print("\n=============================================")  # Visual separator
    print("🚀 STARTING WEATHER BATCH PROCESSOR")  # Banner start message
    print("=============================================\n")  # Visual separator

    spark = create_spark_session()  # Initialize Spark session

    try:
        df = read_data_from_s3(spark)  # Step 1: Read raw weather data

        if df is not None:  # If data exists
            processed_df = process_weather_data(df)  # Step 2: Process/aggregate data
            if processed_df is not None:  # If processing successful
                write_to_s3(processed_df)  # Step 3: Write back to MinIO
                print("✅ Weather data successfully processed and stored.")  # Final success message
            else:
                print("⚠️ Error: Processing returned no results.")  # Warn if processing failed
        else:
            print("⚠️ Error: No data found to process.")  # Warn if read failed

    except Exception as e:  # Handle any fatal errors
        print(f"💥 Fatal error occurred: {str(e)}")  # Log fatal error

    finally:
        print("\n🛑 Stopping Spark Session...")  # Notify stop
        spark.stop()  # Gracefully stop Spark to free resources
        print("✅ Spark Session stopped. Batch Processing Complete.")  # Confirm shutdown
        print("\n=============================================")  # End separator

# ===============================================================
# 8️⃣ Script Entry Point
# Run the main() function only when executed directly.
# ===============================================================
if __name__ == "__main__":  # Check if script is run as standalone (not imported)
    main()  # Execute main workflow
