#!/usr/bin/env python3  # Specifies the Python 3 interpreter for script execution
# -*- coding: utf-8 -*-  # Declares UTF-8 encoding for file text support

"""
Snowflake Loader for Realtime Stream Weather Data
-------------------------------------------------
Reads processed realtime weather data from MinIO (S3A)
for all available city partitions and performs
incremental upsert into a separate Snowflake table
(STREAM_WEATHER_METRICS).
"""  # Multi-line docstring describing this script’s purpose and logic

import logging  # Provides logging utilities for debug/info/error messages
import sys  # Used for system-level operations like exiting or writing to stdout
from datetime import datetime  # Used for generating timestamps
import boto3  # AWS SDK library to connect and interact with S3/MinIO
import pandas as pd  # Pandas for reading parquet files and data manipulation
import numpy as np  # For numerical data operations and type checks
import snowflake.connector  # For connecting and running SQL on Snowflake
import io  # Used to handle byte streams in memory (for reading data from S3)

# ============================================================
# Configuration
# ============================================================
S3_ENDPOINT = "http://localhost:9000"  # URL for MinIO endpoint
S3_ACCESS_KEY = "minioadmin"  # MinIO access key credential
S3_SECRET_KEY = "minioadmin"  # MinIO secret key credential
S3_BUCKET = "weather-data"  # Bucket name in MinIO
S3_PREFIX = "processed/realtime/"  # Folder prefix for realtime processed data

SNOWFLAKE_ACCOUNT = "JBPDBXV-JV54256"  # Snowflake account identifier
SNOWFLAKE_USER = "MANIKANTA033"  # Username for Snowflake
SNOWFLAKE_PASSWORD = "*********"  # Snowflake password
SNOWFLAKE_DATABASE = "WEATHERDATA"  # Target Snowflake database
SNOWFLAKE_SCHEMA = "PUBLIC"  # Target schema inside Snowflake
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # Snowflake virtual warehouse for compute resources
SNOWFLAKE_TABLE = "STREAM_WEATHER_METRICS"  # Table name for realtime data

# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(  # Configures logging behavior
    level=logging.INFO,  # Set global log level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format includes timestamp, level, and message
    handlers=[logging.StreamHandler(sys.stdout)]  # Send log output to standard console
)
logger = logging.getLogger(__name__)  # Create logger instance for this script

# ============================================================
# Initialize MinIO (S3)
# ============================================================
def init_s3_client():  # Function to create a boto3 client for MinIO
    try:
        s3_client = boto3.client(  # Instantiate S3 client
            "s3",  # Service name for S3-compatible client
            endpoint_url=S3_ENDPOINT,  # Set MinIO endpoint URL
            aws_access_key_id=S3_ACCESS_KEY,  # Access key for authentication
            aws_secret_access_key=S3_SECRET_KEY  # Secret key for authentication
        )
        logger.info("✅ MinIO (S3) client initialized successfully.")  # Log successful initialization
        return s3_client  # Return the client object
    except Exception:  # Catch any exception during setup
        logger.exception("❌ Failed to initialize S3 client.")  # Log error with stack trace
        raise  # Re-raise the exception to terminate process

# ============================================================
# Snowflake Connection
# ============================================================
def init_snowflake_connection():  # Function to establish connection to Snowflake
    try:
        conn = snowflake.connector.connect(  # Create Snowflake connection
            user=SNOWFLAKE_USER,  # Provide username
            password=SNOWFLAKE_PASSWORD,  # Provide password
            account=SNOWFLAKE_ACCOUNT,  # Specify account ID
            warehouse=SNOWFLAKE_WAREHOUSE,  # Specify compute warehouse
            database=SNOWFLAKE_DATABASE,  # Target database
            schema=SNOWFLAKE_SCHEMA  # Target schema
        )
        logger.info("✅ Snowflake connection established.")  # Log successful connection
        return conn  # Return the connection object
    except Exception:  # Handle exceptions if connection fails
        logger.exception("❌ Failed to connect to Snowflake.")  # Log failure
        raise  # Re-throw error

def create_stream_table(conn):  # Function to create the realtime stream table in Snowflake if not exists
    """Create separate Snowflake table for stream data."""  # Short docstring for this function
    create_query = f"""  # SQL query to create table if not already existing
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        CITY STRING,
        WINDOW_START TIMESTAMP,
        WINDOW_END TIMESTAMP,
        AVG_TEMP FLOAT,
        MAX_TEMP FLOAT,
        MIN_TEMP FLOAT,
        AVG_HUMIDITY FLOAT,
        AVG_PRESSURE FLOAT,
        MAX_WIND_SPEED FLOAT,
        LAST_UPDATED TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:  # Create a Snowflake cursor
            cur.execute(create_query)  # Execute the CREATE TABLE query
        logger.info(f"✅ Verified/created stream table: {SNOWFLAKE_TABLE}")  # Log success message
    except Exception:  # Catch table creation errors
        logger.exception("❌ Failed to create stream table.")  # Log exception
        raise  # Re-raise to stop further processing

# ============================================================
# Read All Realtime Data from MinIO
# ============================================================
def read_all_realtime_data(s3_client):  # Function to read all realtime data parquet files
    """Read all realtime city Parquet files from MinIO."""  # Docstring describing the function
    logger.info(f"📦 Scanning S3 folder: s3://{S3_BUCKET}/{S3_PREFIX}")  # Log which folder is being read
    dfs = []  # Initialize list to store multiple dataframes

    try:
        paginator = s3_client.get_paginator("list_objects_v2")  # Use paginator for large folder listing
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):  # Iterate through all pages of results
            for obj in page.get("Contents", []):  # Loop through all files in current page
                key = obj["Key"]  # Get file key (path)
                if not key.endswith(".parquet"):  # Skip non-parquet files
                    continue

                try:
                    city = None  # Initialize city variable
                    for part in key.split("/"):  # Split path into parts
                        if part.startswith("city="):  # Identify city folder
                            city = part.split("=", 1)[1]  # Extract city name
                            break

                    data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()  # Read parquet file bytes
                    df = pd.read_parquet(io.BytesIO(data))  # Load bytes into Pandas DataFrame

                    if "city" not in df.columns:  # If city column missing
                        df["city"] = city  # Add city column from folder name
                    df["last_updated"] = datetime.now()  # Add current timestamp to indicate load time

                    dfs.append(df)  # Append DataFrame to list
                    logger.info(f"✔️  Loaded {len(df)} records from {city}")  # Log number of rows loaded per city
                except Exception as e:  # Handle read errors per file
                    logger.error(f"⚠️ Failed to read {key}: {e}")  # Log failed file and error

        if not dfs:  # If no DataFrames were collected
            logger.warning("No realtime data files found.")  # Log warning
            return None  # Exit early

        df_all = pd.concat(dfs, ignore_index=True)  # Combine all DataFrames into one

        required_cols = [  # List of expected columns in final dataset
            "city", "window_start", "window_end", "avg_temp", "max_temp",
            "min_temp", "avg_humidity", "avg_pressure", "max_wind_speed"
        ]
        for col in required_cols:  # Check each required column
            if col not in df_all.columns:  # If missing
                df_all[col] = None  # Add as None (empty column)

        df_all["last_updated"] = datetime.now()  # Update timestamp for full dataset
        df_all.drop_duplicates(subset=["city", "window_start"], keep="last", inplace=True)  # Remove duplicate city/window combos
        df_all = df_all[required_cols + ["last_updated"]]  # Reorder columns as desired

        logger.info(f"📊 Combined {len(df_all)} total records from all cities.")  # Log total combined record count
        return df_all  # Return final combined DataFrame
    except Exception:  # Catch any general MinIO read exceptions
        logger.exception("❌ Error reading realtime data from MinIO.")  # Log stack trace
        return None  # Return None on error

# ============================================================
# Incremental Load to Snowflake
# ============================================================
def incremental_load_to_snowflake(conn, df):  # Function to insert or update realtime data into Snowflake
    logger.info("----------- Performing Incremental Load into Stream Table -----------")  # Log start of process

    if df is None or df.empty:  # Check if DataFrame is empty or None
        logger.info("ℹ️ No realtime data to load.")  # Log info and exit
        return

    try:
        cur = conn.cursor()  # Open Snowflake cursor
        stage_table = "TEMP_STREAM_STAGE"  # Define temporary staging table name
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")  # Create staging table with same schema

        # Uppercase columns to match Snowflake
        cols = [c.upper() for c in df.columns]  # Convert all column names to uppercase
        df.columns = cols  # Apply uppercase names to DataFrame

        # Prepare insert
        records = df.to_records(index=False)  # Convert DataFrame rows to record tuples
        placeholders = ",".join(["%s"] * len(cols))  # Create SQL placeholders for each column
        quoted_cols = ",".join([f'"{c}"' for c in cols])  # Quote column names for SQL syntax
        insert_query = f"INSERT INTO {stage_table} ({quoted_cols}) VALUES ({placeholders})"  # Prepare insert SQL command

        formatted_records = []  # List to hold formatted tuples
        for record in records:  # Iterate through each record
            row = tuple(  # Format each value for Snowflake compatibility
                None if pd.isna(val)  # Replace NaN with None
                else val.to_pydatetime() if isinstance(val, pd.Timestamp)  # Convert Pandas timestamps to Python datetime
                else float(val) if isinstance(val, (np.floating, np.float64))  # Convert floating numbers to Python float
                else int(val) if isinstance(val, (np.integer, np.int64))  # Convert NumPy integers to Python int
                else val  # Leave strings as is
                for val in record
            )
            formatted_records.append(row)  # Add cleaned record tuple to list

        logger.info(f"📥 Inserting {len(formatted_records)} records into staging table...")  # Log number of inserts
        cur.executemany(insert_query, formatted_records)  # Execute bulk insert into staging table

        merge_query = f"""  # Define SQL MERGE command for upsert logic
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.CITY = source.CITY AND target.WINDOW_START = source.WINDOW_START
        WHEN MATCHED THEN UPDATE SET
            target.AVG_TEMP = source.AVG_TEMP,
            target.MAX_TEMP = source.MAX_TEMP,
            target.MIN_TEMP = source.MIN_TEMP,
            target.AVG_HUMIDITY = source.AVG_HUMIDITY,
            target.AVG_PRESSURE = source.AVG_PRESSURE,
            target.MAX_WIND_SPEED = source.MAX_WIND_SPEED,
            target.LAST_UPDATED = source.LAST_UPDATED
        WHEN NOT MATCHED THEN
            INSERT (CITY, WINDOW_START, WINDOW_END, AVG_TEMP, MAX_TEMP, MIN_TEMP,
                    AVG_HUMIDITY, AVG_PRESSURE, MAX_WIND_SPEED, LAST_UPDATED)
            VALUES (source.CITY, source.WINDOW_START, source.WINDOW_END, source.AVG_TEMP,
                    source.MAX_TEMP, source.MIN_TEMP, source.AVG_HUMIDITY, source.AVG_PRESSURE,
                    source.MAX_WIND_SPEED, source.LAST_UPDATED);
        """

        cur.execute(merge_query)  # Execute MERGE statement in Snowflake
        logger.info(f"✅ Incremental load to {SNOWFLAKE_TABLE} completed successfully.")  # Log completion success
    except Exception:  # Handle any SQL or data errors
        logger.exception("❌ Incremental load failed.")  # Log failure with stack trace
    finally:
        cur.close()  # Ensure cursor is closed

# ============================================================
# MAIN
# ============================================================
def main():  # Main entry point for the script
    logger.info("\n=========================================")  # Decorative log header
    logger.info("🌦️  STARTING STREAM WEATHER LOAD (ALL CITIES)")  # Banner for process start
    logger.info("=========================================\n")  # Decorative footer

    s3_client = init_s3_client()  # Initialize S3 (MinIO) client
    conn = init_snowflake_connection()  # Initialize Snowflake connection

    try:
        create_stream_table(conn)  # Create stream table if missing
        df = read_all_realtime_data(s3_client)  # Read all available realtime parquet data
        incremental_load_to_snowflake(conn, df)  # Load dataframe incrementally into Snowflake
    except Exception:  # Handle global fatal errors
        logger.exception("💥 Fatal error during Snowflake stream load.")  # Log detailed traceback
        sys.exit(1)  # Exit script with non-zero code to indicate failure
    finally:
        conn.close()  # Close Snowflake connection
        logger.info("✅ Snowflake connection closed.")  # Confirm closure
        logger.info(f"🎯 Stream weather data load into {SNOWFLAKE_TABLE} complete.")  # Log final completion

# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":  # Execute only when file run directly
    main()  # Call the main function to run the ETL process
