import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os

# ✅ Override any Hadoop defaults that use "60s"
os.environ["HADOOP_OPTS"] = "-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000"
os.environ["fs.s3a.connection.timeout"] = "60000"
os.environ["fs.s3a.connection.establish.timeout"] = "60000"
os.environ["fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"


# ------------------- MinIO / S3 Configuration -------------------
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "weather-data"
MINIO_ENDPOINT = "http://minio:9000"


# ------------------- Spark Session -------------------
def create_spark_session():
    print("Initializing Spark Session with MinIO (S3A) configuration...")

    spark = (
        SparkSession.builder
        .appName("WeatherBatchProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",
        )
        # 🔧 Override Hadoop defaults that cause "For input string: '60s'"
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Configure MinIO access
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session initialized successfully.\n")
    return spark


# ------------------- Read Raw Data from MinIO -------------------
def read_data_from_s3(spark, date=None):
    print("Reading raw weather data from MinIO...")

    process_date = datetime.now() if date is None else datetime.strptime(date, "%Y-%m-%d")
    year, month, day = process_date.year, process_date.month, process_date.day

    # Read all cities within the given date
    s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/year={year}/month={month:02d}/day={day:02d}/city=*/"
    print(f"📂 S3 path: {s3_path}")

    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(s3_path)
        )

        print("✅ Sample data from MinIO:")
        df.select("date", "city", "temperature", "humidity", "wind_speed", "precip_mm").show(5, truncate=False)
        df.printSchema()

        return df
    except Exception as e:
        print(f"❌ Error reading data from S3: {str(e)}")
        return None


# ------------------- Process Weather Data -------------------
def process_weather_data(df):
    print("\n---- Processing Historical Weather Data ----")

    if df is None or df.count() == 0:
        print("⚠️ No data to process.")
        return None

    try:
        record_count = df.count()
        print(f"Total records: {record_count}")

        # Ensure correct schema and types
        df = (
            df.withColumn("date", F.to_date("date"))
              .withColumn("temperature", F.col("temperature").cast("double"))
              .withColumn("humidity", F.col("humidity").cast("double"))
              .withColumn("wind_speed", F.col("wind_speed").cast("double"))
              .withColumn("precip_mm", F.col("precip_mm").cast("double"))
        )

        # Daily aggregations per city
        agg_df = (
            df.groupBy("city", "date")
            .agg(
                F.avg("temperature").alias("avg_temp"),
                F.max("temperature").alias("max_temp"),
                F.min("temperature").alias("min_temp"),
                F.avg("humidity").alias("avg_humidity"),
                F.max("wind_speed").alias("max_wind_speed"),
                F.sum("precip_mm").alias("total_precip_mm"),
            )
            .orderBy("city", "date")
        )

        print("📊 Sample of processed daily metrics:")
        agg_df.show(10, truncate=False)
        return agg_df

    except Exception as e:
        print(f"❌ Error processing weather data: {str(e)}")
        return None


# ------------------- Write Results to MinIO -------------------
def write_to_s3(df, date=None):
    print("\n------ Writing processed weather data to MinIO ------")

    if df is None:
        print("⚠️ No data to write.")
        return None

    processed_date = datetime.now().strftime("%Y-%m-%d") if date is None else date
    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/date={processed_date}/"

    print(f"💾 Output path: {output_path}")
    try:
        (
            df.write.partitionBy("city")
            .mode("overwrite")
            .parquet(output_path)
        )
        print(f"✅ Data written to MinIO at: {output_path}")
    except Exception as e:
        print(f"❌ Error writing to MinIO: {str(e)}")


# ------------------- Main Driver -------------------
def main():
    print("\n=============================================")
    print("🚀 STARTING WEATHER BATCH PROCESSOR")
    print("=============================================\n")

    spark = create_spark_session()

    try:
        df = read_data_from_s3(spark)

        if df is not None:
            processed_df = process_weather_data(df)
            if processed_df is not None:
                write_to_s3(processed_df)
                print("✅ Weather data successfully processed and stored.")
            else:
                print("⚠️ Error: Processing returned no results.")
        else:
            print("⚠️ Error: No data found to process.")

    except Exception as e:
        print(f"💥 Fatal error occurred: {str(e)}")
    finally:
        print("\n🛑 Stopping Spark Session...")
        spark.stop()
        print("✅ Spark Session stopped. Batch Processing Complete.")
        print("\n=============================================")


if __name__ == "__main__":
    main()
