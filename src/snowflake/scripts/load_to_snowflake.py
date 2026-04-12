#!/usr/bin/env python3  # Specifies the Python 3 interpreter for script execution
# -*- coding: utf-8 -*-  # Declares UTF-8 encoding to support all characters

"""
Snowflake Incremental Loader for Weather Data
---------------------------------------------
Reads processed weather data from MinIO (S3A) and
performs incremental upsert into Snowflake for analytics.
(No window_start or window_end fields)
"""  # Multi-line docstring describing the script’s purpose and scope

import logging  # For logging progress, info, and error messages
import sys  # Provides access to system-specific parameters and functions
from datetime import datetime  # Used to manage and insert timestamps
import boto3  # AWS SDK library for interacting with S3 (and MinIO)
import pandas as pd  # Data manipulation library
import numpy as np  # Numerical computations and data type handling
import snowflake.connector  # Connector for Snowflake database access
import io  # Allows in-memory file operations (used for reading files directly from S3)

# ============================================================
#                 Configuration
# ============================================================
S3_ENDPOINT = "http://localhost:9000"  # URL for local MinIO server (S3-compatible)
S3_ACCESS_KEY = "minioadmin"  # MinIO access key for authentication
S3_SECRET_KEY = "minioadmin"  # MinIO secret key for authentication
S3_BUCKET = "weather-data"  # Name of the S3 bucket used to store weather data

SNOWFLAKE_ACCOUNT = "JBPDBXV-JV54256"  # Snowflake account identifier
SNOWFLAKE_USER = "MANIKANTA033"  # Snowflake username
SNOWFLAKE_PASSWORD = "********"  # Snowflake password 
SNOWFLAKE_DATABASE = "WEATHERDATA"  # Target database in Snowflake
SNOWFLAKE_SCHEMA = "PUBLIC"  # Target schema in Snowflake
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # Virtual warehouse for compute resources
SNOWFLAKE_TABLE = "DAILY_WEATHER_METRICS"  # Destination table for incremental data loads

# ============================================================
#                 Logging Setup
# ============================================================
logging.basicConfig(  # Configure logging for the application
    level=logging.INFO,  # Set log level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define timestamped log format
    handlers=[logging.StreamHandler(sys.stdout)]  # Output logs to console (stdout)
)
logger = logging.getLogger(__name__)  # Create a logger object for this module

# ============================================================
#                 S3 (MinIO) Initialization
# ============================================================
def init_s3_client():  # Function to initialize and return a boto3 S3 client
    try:
        s3_client = boto3.client(  # Create an S3 client object
            "s3",  # Specify service name
            endpoint_url=S3_ENDPOINT,  # Connect to MinIO endpoint
            aws_access_key_id=S3_ACCESS_KEY,  # Provide MinIO access key
            aws_secret_access_key=S3_SECRET_KEY  # Provide MinIO secret key
        )
        logger.info("✅ MinIO (S3) client initialized successfully.")  # Log success
        return s3_client  # Return configured S3 client
    except Exception:  # Handle initialization errors
        logger.exception("❌ Failed to initialize S3 client.")  # Log error with traceback
        raise  # Propagate exception further

# ============================================================
#                 Snowflake Connection
# ============================================================
def init_snowflake_connection():  # Function to initialize Snowflake DB connection
    try:
        conn = snowflake.connector.connect(  # Establish Snowflake connection
            user=SNOWFLAKE_USER,  # Snowflake username
            password=SNOWFLAKE_PASSWORD,  # Snowflake password
            account=SNOWFLAKE_ACCOUNT,  # Snowflake account name
            warehouse=SNOWFLAKE_WAREHOUSE,  # Specify virtual warehouse
            database=SNOWFLAKE_DATABASE,  # Specify target database
            schema=SNOWFLAKE_SCHEMA  # Specify schema
        )
        logger.info("✅ Snowflake connection established.")  # Log success
        return conn  # Return database connection object
    except Exception:  # Catch exceptions on connection failure
        logger.exception("❌ Failed to connect to Snowflake.")  # Log detailed error
        raise  # Re-raise exception

def create_snowflake_table(conn):  # Function to ensure target table exists in Snowflake
    """Create weather metrics table if it doesn't exist."""  # Docstring describing the function
    create_query = f"""  # Define SQL statement to create table if not present
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        city STRING PRIMARY KEY,
        avg_temp FLOAT,
        max_temp FLOAT,
        min_temp FLOAT,
        avg_humidity FLOAT,
        avg_pressure FLOAT,
        max_wind_speed FLOAT,
        last_updated TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:  # Use Snowflake cursor context
            cur.execute(create_query)  # Execute CREATE TABLE statement
        logger.info("✅ Verified/created Snowflake table ")  # Log table creation success
    except Exception:  # Catch any SQL execution error
        logger.exception("❌ Failed to create Snowflake table.")  # Log exception
        raise  # Propagate error

# ============================================================
#            Read Processed Data from MinIO
# ============================================================
def read_processed_data(s3_client, subfolder="historical"):  # Function to read processed parquet data from MinIO
    """
    Read Parquet files from MinIO for processed data.
    Handles partition paths like:
    processed/historical/date=2025-11-09/city=Miami,FL,USA/
    """  # Docstring describing functionality
    s3_prefix = f"processed/{subfolder}/"  # Build prefix path for MinIO folder
    logger.info(f"📦 Reading from s3://{S3_BUCKET}/{s3_prefix}")  # Log S3 path

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)  # List objects from MinIO under prefix
        if "Contents" not in response:  # Check if any files exist
            logger.warning(f"No files found in {s3_prefix}")  # Log warning if none found
            return None  # Exit function

        dfs = []  # List to store dataframes
        for obj in response["Contents"]:  # Iterate through all files in bucket
            key = obj["Key"]  # Extract file key (path)
            if not key.endswith(".parquet"):  # Skip non-parquet files
                continue

            try:
                # ✅ Extract city from path
                city = None  # Initialize city variable
                for part in key.split("/"):  # Split file path by '/'
                    if part.startswith("city="):  # Check if directory contains 'city='
                        city = part.split("=", 1)[1]  # Extract city name
                        break

                data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()  # Download file data
                df = pd.read_parquet(io.BytesIO(data))  # Load Parquet data into Pandas DataFrame

                # Add city column if missing
                if "city" not in df.columns:  # If city column is missing
                    df["city"] = city  # Add city column

                # Keep only metrics columns
                keep_cols = [  # Define required columns
                    "city", "avg_temp", "max_temp", "min_temp",
                    "avg_humidity", "avg_pressure", "max_wind_speed"
                ]
                for col in keep_cols:  # Ensure all columns exist
                    if col not in df.columns:
                        df[col] = 0  # Fill missing column with 0

                df = df[keep_cols]  # Retain only required columns
                dfs.append(df)  # Add dataframe to list
                logger.info(f"✔️  Loaded file for city={city}")  # Log file load success
            except Exception as e:  # Catch errors for specific files
                logger.error(f"⚠️ Failed to read {key}: {e}")  # Log failure for file

        if not dfs:  # Check if any dataframes were loaded
            logger.warning("No data frames loaded from MinIO.")  # Log warning if empty
            return None  # Exit function

        df_all = pd.concat(dfs, ignore_index=True)  # Concatenate all city dataframes
        df_all["last_updated"] = datetime.now()  # Add current timestamp
        df_all.drop_duplicates(subset=["city"], keep="last", inplace=True)  # Remove duplicate cities
        df_all.columns = [c.lower() for c in df_all.columns]  # Convert all column names to lowercase

        logger.info(f"📊 Loaded {len(df_all)} rows from MinIO ({subfolder})")  # Log total rows loaded
        return df_all  # Return combined dataframe
    except Exception:  # Handle any other S3 read errors
        logger.exception("❌ Error reading processed data from MinIO.")  # Log failure
        return None  # Return None on error

# ============================================================
#                 Incremental Load to Snowflake
# ============================================================
def incremental_load_to_snowflake(conn, df):  # Function to perform incremental merge into Snowflake
    logger.info("----------- Performing Incremental Load into Snowflake -----------")  # Log operation start

    if df is None or df.empty:  # Check for empty dataframe
        logger.info("ℹ️ No data to load.")  # Log info
        return  # Exit early

    try:
        cur = conn.cursor()  # Create cursor for executing SQL commands

        # ✅ Create temporary staging table (no window fields)
        stage_table = "TEMP_WEATHER_STAGE"  # Name of temporary table
        cur.execute(f"""  # Execute table creation query
            CREATE OR REPLACE TEMPORARY TABLE {stage_table} (
                city STRING,
                avg_temp FLOAT,
                max_temp FLOAT,
                min_temp FLOAT,
                avg_humidity FLOAT,
                avg_pressure FLOAT,
                max_wind_speed FLOAT,
                last_updated TIMESTAMP
            )
        """)

        # Prepare tuples for batch insert
        records = df.to_records(index=False)  # Convert dataframe to numpy record array
        cols = list(df.columns)  # Extract list of column names
        placeholders = ",".join(["%s"] * len(cols))  # Prepare placeholders for SQL insert
        insert_query = f"INSERT INTO {stage_table} ({','.join(cols)}) VALUES ({placeholders})"  # Create SQL insert query

        formatted_records = []  # Initialize list for cleaned records
        for record in records:  # Iterate through all rows
            row = tuple(  # Clean each field for insertion
                0 if pd.isna(val) or val == "" else  # Replace NaN or empty with 0
                float(val) if isinstance(val, (np.floating, np.float64))  # Convert floating numbers
                else int(val) if isinstance(val, (np.integer, np.int64))  # Convert integers
                else val  # Keep string or timestamp as-is
                for val in record
            )
            formatted_records.append(row)  # Append cleaned tuple

        logger.info(f"📥 Inserting {len(formatted_records)} records into staging table...")  # Log insert size
        cur.executemany(insert_query, formatted_records)  # Insert all rows in batch mode

        # ✅ Merge logic based only on city
        merge_query = f"""  # Define Snowflake MERGE SQL query for upsert operation
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.city = source.city
        WHEN MATCHED THEN UPDATE SET
            target.avg_temp = source.avg_temp,
            target.max_temp = source.max_temp,
            target.min_temp = source.min_temp,
            target.avg_humidity = source.avg_humidity,
            target.avg_pressure = source.avg_pressure,
            target.max_wind_speed = source.max_wind_speed,
            target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN
            INSERT (city, avg_temp, max_temp, min_temp, avg_humidity,
                    avg_pressure, max_wind_speed, last_updated)
            VALUES (source.city, source.avg_temp, source.max_temp, source.min_temp,
                    source.avg_humidity, source.avg_pressure, source.max_wind_speed,
                    source.last_updated);
        """

        cur.execute(merge_query)  # Execute merge query
        logger.info("✅ Incremental load to Snowflake completed successfully.")  # Log completion
    except Exception:  # Catch any database errors
        logger.exception("❌ Incremental load failed.")  # Log error with traceback
    finally:
        cur.close()  # Close cursor

# ============================================================
#                        MAIN
# ============================================================
def main():  # Entry function for running full ETL
    logger.info("\n=========================================")  # Print header separator
    logger.info("🌦️  STARTING SNOWFLAKE WEATHER LOAD (NO WINDOW COLUMNS)")  # Log start banner
    logger.info("=========================================\n")  # Print footer separator

    s3_client = init_s3_client()  # Initialize S3 client
    conn = init_snowflake_connection()  # Connect to Snowflake

    try:
        create_snowflake_table(conn)  # Ensure target table exists

        # ✅ Only load historical data
        df = read_processed_data(s3_client, subfolder="historical")  # Read processed weather data
        if df is not None:  # Proceed if dataframe loaded
            incremental_load_to_snowflake(conn, df)  # Perform upsert into Snowflake
        else:
            logger.info("ℹ️ No data found in historical/")  # Log if nothing found

    except Exception:  # Catch any fatal ETL errors
        logger.exception("💥 Fatal error during Snowflake load.")  # Log stack trace
        sys.exit(1)  # Exit script with error code
    finally:
        conn.close()  # Close Snowflake connection
        logger.info("✅ Snowflake connection closed.")  # Confirm closure
        logger.info("🎯 Weather data load process complete.")  # Log successful end

# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":  # Run only if executed as main script
    main()  # Invoke main ETL function
