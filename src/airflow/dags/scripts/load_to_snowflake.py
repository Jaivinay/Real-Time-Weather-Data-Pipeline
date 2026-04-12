# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Snowflake Incremental Loader for Weather Data
# ---------------------------------------------
# Reads processed weather data from MinIO (S3A) and
# performs incremental upsert into Snowflake for analytics.
# """

# import logging
# import sys
# import traceback
# from datetime import datetime
# import boto3
# import pandas as pd
# import numpy as np
# import snowflake.connector
# import io

# # ============================================================
# #                 Configuration
# # ============================================================
# S3_ENDPOINT = "http://localhost:9000"       # Docker network endpoint
# S3_ACCESS_KEY = "minioadmin"
# S3_SECRET_KEY = "minioadmin"
# S3_BUCKET = "weather-data"

# SNOWFLAKE_ACCOUNT = "JBPDBXV-JV54256"
# SNOWFLAKE_USER = "MANIKANTA033"
# SNOWFLAKE_PASSWORD = "9989185668@Manik"  # Ideally from environment variable
# SNOWFLAKE_DATABASE = "WEATHERDATA"
# SNOWFLAKE_SCHEMA = "PUBLIC"
# SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
# SNOWFLAKE_TABLE = "DAILY_WEATHER_METRICS"

# # ============================================================
# #                 Logging Setup
# # ============================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)]
# )
# logger = logging.getLogger(__name__)

# # ============================================================
# #                 S3 (MinIO) Initialization
# # ============================================================
# def init_s3_client():
#     try:
#         s3_client = boto3.client(
#             "s3",
#             endpoint_url=S3_ENDPOINT,
#             aws_access_key_id=S3_ACCESS_KEY,
#             aws_secret_access_key=S3_SECRET_KEY
#         )
#         logger.info("✅ MinIO (S3) client initialized successfully.")
#         return s3_client
#     except Exception:
#         logger.exception("❌ Failed to initialize S3 client.")
#         raise

# # ============================================================
# #                 Snowflake Connection
# # ============================================================
# def init_snowflake_connection():
#     try:
#         conn = snowflake.connector.connect(
#             user=SNOWFLAKE_USER,
#             password=SNOWFLAKE_PASSWORD,
#             account=SNOWFLAKE_ACCOUNT,
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA
#         )
#         logger.info("✅ Snowflake connection established.")
#         return conn
#     except Exception:
#         logger.exception("❌ Failed to connect to Snowflake.")
#         raise

# def create_snowflake_table(conn):
#     """Create weather metrics table if it doesn't exist."""
#     create_query = f"""
#     CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
#         city STRING,
#         # window_start TIMESTAMP,
#         # window_end TIMESTAMP,
#         avg_temp FLOAT,
#         max_temp FLOAT,
#         min_temp FLOAT,
#         avg_humidity FLOAT,
#         avg_pressure FLOAT,
#         max_wind_speed FLOAT,
#         last_updated TIMESTAMP,
#         PRIMARY KEY (city, window_start)
#     );
#     """
#     try:
#         with conn.cursor() as cur:
#             cur.execute(create_query)
#         logger.info("✅ Verified/created Snowflake table.")
#     except Exception:
#         logger.exception("❌ Failed to create Snowflake table.")
#         raise

# # ============================================================
# #            Read Processed Data from MinIO
# # ============================================================
# def read_processed_data(s3_client, subfolder="realtime"):
#     """
#     Read Parquet files from MinIO for today's processed data.
#     Handles partition paths like:
#     processed/historical/date=2025-11-09/city=Miami,FL,USA/
#     """
#     s3_prefix = f"processed/{subfolder}/"
#     logger.info(f"📦 Reading from s3://{S3_BUCKET}/{s3_prefix}")

#     try:
#         response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
#         if "Contents" not in response:
#             logger.warning(f"No files found in {s3_prefix}")
#             return None

#         dfs = []
#         for obj in response["Contents"]:
#             key = obj["Key"]
#             if not key.endswith(".parquet"):
#                 continue

#             try:
#                 # ✅ Extract city from partitioned path: city=Miami,FL,USA
#                 city = None
#                 for part in key.split("/"):
#                     if part.startswith("city="):
#                         city = part.split("=", 1)[1]
#                         break

#                 data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
#                 df = pd.read_parquet(io.BytesIO(data))

#                 # Add city column if missing
#                 if "city" not in df.columns:
#                     df["city"] = city

#                 dfs.append(df)
#                 logger.info(f"✔️  Loaded file for city={city}")
#             except Exception as e:
#                 logger.error(f"⚠️ Failed to read {key}: {e}")

#         if not dfs:
#             logger.warning("No data frames loaded from MinIO.")
#             return None

#         df_all = pd.concat(dfs, ignore_index=True)
#         logger.info(f"📊 Loaded {len(df_all)} rows from MinIO ({subfolder})")

#         # Ensure proper schema alignment
#         required_cols = [
#             "city", "window_start", "window_end", "avg_temp", "max_temp",
#             "min_temp", "avg_humidity", "avg_pressure", "max_wind_speed"
#         ]
#         for col in required_cols:
#             if col not in df_all.columns:
#                 df_all[col] = None

#         df_all["last_updated"] = datetime.now()
#         df_all.drop_duplicates(subset=["city", "window_start"], keep="last", inplace=True)

#         df_all = df_all[required_cols + ["last_updated"]]
#         df_all.columns = [c.lower() for c in df_all.columns]

#         return df_all
#     except Exception:
#         logger.exception("❌ Error reading processed data from MinIO.")
#         return None

# # ============================================================
# #                 Incremental Load to Snowflake
# # ============================================================
# def incremental_load_to_snowflake(conn, df):
#     logger.info("----------- Performing Incremental Load into Snowflake -----------")

#     if df is None or df.empty:
#         logger.info("ℹ️ No data to load.")
#         return

#     try:
#         cur = conn.cursor()
#         stage_table = "TEMP_WEATHER_STAGE"
#         cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")

#         # Prepare tuples for batch insert
#         records = df.to_records(index=False)
#         cols = list(df.columns)
#         placeholders = ",".join(["%s"] * len(cols))
#         insert_query = f"INSERT INTO {stage_table} ({','.join(cols)}) VALUES ({placeholders})"

#         formatted_records = []
#         for record in records:
#             row = tuple(
#                 None if pd.isna(val)
#                 else val.to_pydatetime() if isinstance(val, pd.Timestamp)
#                 else float(val) if isinstance(val, (np.floating, np.float64))
#                 else int(val) if isinstance(val, (np.integer, np.int64))
#                 else val
#                 for val in record
#             )
#             formatted_records.append(row)

#         logger.info(f"📥 Inserting {len(formatted_records)} records into staging table...")
#         cur.executemany(insert_query, formatted_records)

#         merge_query = f"""
#         MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
#         USING {stage_table} AS source
#         ON target.city = source.city AND target.window_start = source.window_start
#         WHEN MATCHED THEN UPDATE SET
#             target.avg_temp = source.avg_temp,
#             target.max_temp = source.max_temp,
#             target.min_temp = source.min_temp,
#             target.avg_humidity = source.avg_humidity,
#             target.avg_pressure = source.avg_pressure,
#             target.max_wind_speed = source.max_wind_speed,
#             target.last_updated = source.last_updated
#         WHEN NOT MATCHED THEN
#             INSERT (city, window_start, window_end, avg_temp, max_temp, min_temp,
#                     avg_humidity, avg_pressure, max_wind_speed, last_updated)
#             VALUES (source.city, source.window_start, source.window_end, source.avg_temp,
#                     source.max_temp, source.min_temp, source.avg_humidity, source.avg_pressure,
#                     source.max_wind_speed, source.last_updated);
#         """

#         cur.execute(merge_query)
#         logger.info("✅ Incremental load to Snowflake completed successfully.")
#     except Exception:
#         logger.exception("❌ Incremental load failed.")
#     finally:
#         cur.close()

# # ============================================================
# #                        MAIN
# # ============================================================
# def main():
#     logger.info("\n=========================================")
#     logger.info("🌦️  STARTING SNOWFLAKE WEATHER LOAD")
#     logger.info("=========================================\n")

#     s3_client = init_s3_client()
#     conn = init_snowflake_connection()

#     try:
#         create_snowflake_table(conn)

#         # Load both batch and realtime processed data
#         for subfolder in ["historical", "realtime"]:
#             df = read_processed_data(s3_client, subfolder=subfolder)
#             if df is not None:
#                 incremental_load_to_snowflake(conn, df)
#             else:
#                 logger.info(f"ℹ️ No data found in {subfolder}/")

#     except Exception:
#         logger.exception("💥 Fatal error during Snowflake load.")
#         sys.exit(1)
#     finally:
#         conn.close()
#         logger.info("✅ Snowflake connection closed.")
#         logger.info("🎯 Weather data load process complete.")

# # ============================================================
# # Entry Point
# # ============================================================
# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snowflake Incremental Loader for Weather Data
---------------------------------------------
Reads processed weather data from MinIO (S3A) and
performs incremental upsert into Snowflake for analytics.
(No window_start or window_end fields)
"""

import logging
import sys
from datetime import datetime
import boto3
import pandas as pd
import numpy as np
import snowflake.connector
import io

# ============================================================
#                 Configuration
# ============================================================
S3_ENDPOINT = "http://minio:9000"  # Local MinIO endpoint
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "weather-data"

SNOWFLAKE_ACCOUNT = "JBPDBXV-JV54256"
SNOWFLAKE_USER = "MANIKANTA033"
SNOWFLAKE_PASSWORD = "9989185668@Manik"  # Ideally load from environment
SNOWFLAKE_DATABASE = "WEATHERDATA"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "DAILY_WEATHER_METRICS"

# ============================================================
#                 Logging Setup
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================
#                 S3 (MinIO) Initialization
# ============================================================
def init_s3_client():
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        logger.info("✅ MinIO (S3) client initialized successfully.")
        return s3_client
    except Exception:
        logger.exception("❌ Failed to initialize S3 client.")
        raise

# ============================================================
#                 Snowflake Connection
# ============================================================
def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("✅ Snowflake connection established.")
        return conn
    except Exception:
        logger.exception("❌ Failed to connect to Snowflake.")
        raise


def create_snowflake_table(conn):
    """Create weather metrics table if it doesn't exist."""
    create_query = f"""
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
        with conn.cursor() as cur:
            cur.execute(create_query)
        logger.info("✅ Verified/created Snowflake table ")
    except Exception:
        logger.exception("❌ Failed to create Snowflake table.")
        raise

# ============================================================
#            Read Processed Data from MinIO
# ============================================================
def read_processed_data(s3_client, subfolder="historical"):
    """
    Read Parquet files from MinIO for processed data.
    Handles partition paths like:
    processed/historical/date=2025-11-09/city=Miami,FL,USA/
    """
    s3_prefix = f"processed/{subfolder}/"
    logger.info(f"📦 Reading from s3://{S3_BUCKET}/{s3_prefix}")

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        if "Contents" not in response:
            logger.warning(f"No files found in {s3_prefix}")
            return None

        dfs = []
        for obj in response["Contents"]:
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue

            try:
                # ✅ Extract city from path
                city = None
                for part in key.split("/"):
                    if part.startswith("city="):
                        city = part.split("=", 1)[1]
                        break

                data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                df = pd.read_parquet(io.BytesIO(data))

                # Add city column if missing
                if "city" not in df.columns:
                    df["city"] = city

                # Keep only metrics columns
                keep_cols = [
                    "city", "avg_temp", "max_temp", "min_temp",
                    "avg_humidity", "avg_pressure", "max_wind_speed"
                ]
                for col in keep_cols:
                    if col not in df.columns:
                        df[col] = 0

                df = df[keep_cols]
                dfs.append(df)
                logger.info(f"✔️  Loaded file for city={city}")
            except Exception as e:
                logger.error(f"⚠️ Failed to read {key}: {e}")

        if not dfs:
            logger.warning("No data frames loaded from MinIO.")
            return None

        df_all = pd.concat(dfs, ignore_index=True)
        df_all["last_updated"] = datetime.now()
        df_all.drop_duplicates(subset=["city"], keep="last", inplace=True)
        df_all.columns = [c.lower() for c in df_all.columns]

        logger.info(f"📊 Loaded {len(df_all)} rows from MinIO ({subfolder})")
        return df_all
    except Exception:
        logger.exception("❌ Error reading processed data from MinIO.")
        return None

# ============================================================
#                 Incremental Load to Snowflake
# ============================================================
def incremental_load_to_snowflake(conn, df):
    logger.info("----------- Performing Incremental Load into Snowflake -----------")

    if df is None or df.empty:
        logger.info("ℹ️ No data to load.")
        return

    try:
        cur = conn.cursor()

        # ✅ Create temporary staging table (no window fields)
        stage_table = "TEMP_WEATHER_STAGE"
        cur.execute(f"""
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
        records = df.to_records(index=False)
        cols = list(df.columns)
        placeholders = ",".join(["%s"] * len(cols))
        insert_query = f"INSERT INTO {stage_table} ({','.join(cols)}) VALUES ({placeholders})"

        formatted_records = []
        for record in records:
            row = tuple(
                0 if pd.isna(val) or val == "" else
                float(val) if isinstance(val, (np.floating, np.float64))
                else int(val) if isinstance(val, (np.integer, np.int64))
                else val
                for val in record
            )
            formatted_records.append(row)

        logger.info(f"📥 Inserting {len(formatted_records)} records into staging table...")
        cur.executemany(insert_query, formatted_records)

        # ✅ Merge logic based only on city
        merge_query = f"""
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

        cur.execute(merge_query)
        logger.info("✅ Incremental load to Snowflake completed successfully.")
    except Exception:
        logger.exception("❌ Incremental load failed.")
    finally:
        cur.close()

# ============================================================
#                        MAIN
# ============================================================
def main():
    logger.info("\n=========================================")
    logger.info("🌦️  STARTING SNOWFLAKE WEATHER LOAD (NO WINDOW COLUMNS)")
    logger.info("=========================================\n")

    s3_client = init_s3_client()
    conn = init_snowflake_connection()

    try:
        create_snowflake_table(conn)

        # ✅ Only load historical data
        df = read_processed_data(s3_client, subfolder="historical")
        if df is not None:
            incremental_load_to_snowflake(conn, df)
        else:
            logger.info("ℹ️ No data found in historical/")

    except Exception:
        logger.exception("💥 Fatal error during Snowflake load.")
        sys.exit(1)
    finally:
        conn.close()
        logger.info("✅ Snowflake connection closed.")
        logger.info("🎯 Weather data load process complete.")

# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    main()
