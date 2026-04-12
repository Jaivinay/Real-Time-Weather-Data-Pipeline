import json                                   # For converting Python objects (dicts) to JSON strings before sending to Kafka
import logging                                # For standardized logging of information, warnings, and errors
import os                                     # To read environment variables and handle OS-level paths
import time                                   # For introducing time delays (e.g., between API retries)
from datetime import datetime, timedelta, UTC # For managing timestamps and calculating date ranges in UTC
from typing import Optional                   # For type hints (to indicate functions may return None)
from io import StringIO                       # To treat string data as a file-like object for pandas CSV reading

import pandas as pd                           # For loading, cleaning, and processing tabular data
import requests                               # For making HTTP API calls (to Visual Crossing)
from confluent_kafka import Producer          # Kafka producer library to publish messages to Kafka topics
from dotenv import load_dotenv                # Loads key-value pairs from a .env file into environment variables

# ---------------- Load Environment Variables ----------------
load_dotenv()                                 # Loads variables like KAFKA configs from a .env file into the current process

# ---------------- Configure Logging ----------------
logging.basicConfig(
    level=logging.INFO,                       # Log all messages with level INFO or higher (INFO, WARNING, ERROR)
    format="%(asctime)s [%(levelname)s] %(message)s",  # Adds timestamp and level to each log message
)
logger = logging.getLogger(__name__)          # Creates a logger named after the current module for structured logging

# ---------------- Kafka Configuration ----------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
# Reads Kafka broker address from environment, defaulting to localhost port 29092

KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", "weather-batch")
# Reads Kafka topic name (for batch/historical data) from environment variables

# ---------------- Visual Crossing API ----------------
VISUAL_CROSSING_API_KEY = "<SECRET>"          # API key (masked here); required for authenticating with Visual Crossing API
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
# Base endpoint for Visual Crossing's timeline API, which supports range queries for weather history

# ---------------- Cities to Collect Historical Weather ----------------
CITIES = [
    "Tampa,FL,USA",
    "Miami,FL,USA",
    "Orlando,FL,USA",
    "Jacksonville,FL,USA",
    "Tallahassee,FL,USA"
]
# List of target cities in Florida whose historical data will be fetched each week

# ======================================================================
#                         CLASS DEFINITION
# ======================================================================
class HistoricalDataCollector:
    """
    This class handles the end-to-end process of:
    1. Fetching historical weather data from Visual Crossing API
    2. Cleaning the data into a standard schema
    3. Producing it as JSON messages into a Kafka topic
    """

    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH):
        # Initialize Kafka connection parameters
        self.topic = topic
        conf = {"bootstrap.servers": bootstrap_servers, "client.id": "historical-weather-producer-0"}
        # Configuration dict for Kafka producer: specifies broker address and a client ID for logging

        try:
            self.producer = Producer(conf)                   # Create a Kafka producer instance
            logger.info(f"✅ Producer initialized → {bootstrap_servers}")  # Confirm successful producer creation
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka Producer: {e}")       # Log any Kafka initialization error
            raise                                                        # Reraise exception to stop the program

    # ------------------------------------------------------------------
    def fetch_historical_data(
        self, city: str, start_date: str, end_date: str, include: str = "days"
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical weather data for a given city and date range.
        Uses Visual Crossing's API and returns a Pandas DataFrame.
        """
        logger.info(f"🌦️ Fetching {include}-level data for {city} ({start_date} → {end_date})")

        # Construct full API request URL using f-strings
        url = (
            f"{BASE_URL}/{city}/{start_date}/{end_date}"
            f"?key={VISUAL_CROSSING_API_KEY}&unitGroup=metric"
            f"&include={include}&contentType=csv"
        )
        headers = {"User-Agent": "WeatherDataCollector/1.0"}  # Add custom header to identify client

        # Retry loop for transient errors like rate limiting or network failures
        for attempt in range(3):  # Attempt up to 3 times
            try:
                response = requests.get(url, headers=headers, timeout=60)  # HTTP GET with 1-min timeout
                if response.status_code == 429:  # HTTP 429 = Too many requests (rate limit)
                    wait_time = 10 * (attempt + 1)  # Exponential backoff wait
                    logger.warning(f"⏳ Rate limit for {city}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)  # Wait before retrying
                    continue
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx/5xx)
                break  # Exit retry loop on success
            except requests.RequestException as e:
                if attempt < 2:  # Retry up to 2 times
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"⚠️ API error for {city}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Failed after 3 attempts for {city}: {e}")
                    return None  # Return None to signal failure after all retries

        # Convert CSV text from API response into a pandas DataFrame
        df = pd.read_csv(StringIO(response.text))  # Treat response text as file-like buffer
        if df.empty:                               # Check if DataFrame is empty
            logger.warning(f"⚠️ No data returned for {city}")  # Warn if no rows found
            return None

        # Clean column names (lowercase and stripped)
        df.columns = [c.strip().lower() for c in df.columns]
        # Rename specific columns to consistent names used in downstream processing
        df.rename(
            columns={
                "datetime": "timestamp",   # Date column → timestamp
                "temp": "temperature",     # Temp column → temperature
                "windspeed": "wind_speed", # Wind speed
                "precip": "precip_mm",     # Precipitation in millimeters
            },
            inplace=True,
        )
        df["city"] = city  # Add city column for partitioning downstream
        # Convert timestamp column to 'date' string (YYYY-MM-DD)
        df["date"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")

        logger.info(f"✅ Retrieved {len(df)} records for {city}")  # Log number of rows fetched
        return df  # Return the cleaned DataFrame

    # ------------------------------------------------------------------
    def delivery_report(self, err, msg):
        """Callback executed after Kafka message delivery."""
        if err:
            logger.error(f"❌ Delivery failed: {err}")   # Log if Kafka delivery fails
        else:
            logger.debug(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")  # Log success (debug level)

    # ------------------------------------------------------------------
    def produce_to_kafka(self, df: pd.DataFrame, city: str):
        """
        Publish a DataFrame of weather records to Kafka topic.
        Each record is serialized as JSON and keyed by city name.
        """
        batch_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")  # Unique ID for batch (timestamp)
        df["batch_id"] = batch_id                               # Add batch identifier column
        df["batch_date"] = datetime.now(UTC).strftime("%Y-%m-%d")  # Add human-readable date column

        success, fail = 0, 0  # Counters for successful and failed Kafka sends

        # Convert DataFrame rows to list of dictionaries for iteration
        for record in df.to_dict(orient="records"):
            try:
                message = json.dumps(record)  # Serialize record to JSON
                self.producer.produce(
                    topic=self.topic,          # Kafka topic to publish to
                    key=city.encode(),         # Partition key as bytes (city name)
                    value=message.encode(),    # Message payload as bytes (JSON string)
                    callback=self.delivery_report,  # Attach callback for delivery confirmation
                )
                self.producer.poll(0)          # Trigger callbacks without blocking
                success += 1                   # Count as successful send
            except Exception as e:
                logger.error(f"❌ Failed to send record for {city}: {e}")  # Log any send failure
                fail += 1                     # Increment failure counter

        self.producer.flush()                  # Ensure all messages are sent before proceeding
        logger.info(f"📦 Produced {success} records for {city} (failed {fail})")  # Log production summary

    # ------------------------------------------------------------------
    def collect_historical_data(self, start_date: str, end_date: str):
        """
        Main method to loop through all configured cities,
        fetch their historical weather data, and push it to Kafka.
        """
        logger.info(f"🚀 Collecting weekly weather data for {len(CITIES)} cities")

        success, fail = 0, 0  # Counters for city-level success/failure
        for city in CITIES:   # Iterate through each configured city
            df = self.fetch_historical_data(city, start_date, end_date)  # Fetch data for city
            if df is not None and not df.empty:  # If data successfully retrieved
                self.produce_to_kafka(df, city)  # Send to Kafka
                success += 1
            else:
                fail += 1                        # Increment fail counter for missing data
            time.sleep(5)  # Pause 5 seconds between cities to avoid hitting API rate limits

        # Summary log after all cities processed
        logger.info(f"🏁 Weekly collection complete — Success={success}, Fail={fail}")

# ======================================================================
#                            MAIN FUNCTION
# ======================================================================
def main():
    """Script entry point to initialize the collector and start data ingestion."""
    try:
        logger.info("Starting Weekly Weather Data Collector (CSV API version)")

        # Initialize the HistoricalDataCollector with Kafka settings
        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH
        )

        # ----- Define a one-week observation window -----
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")           # Today’s date in UTC
        start_date = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")  # Seven days ago

        # Begin collection for all cities over the date range
        collector.collect_historical_data(start_date=start_date, end_date=end_date)

    except Exception as e:
        # Capture and log any unexpected fatal errors
        logger.error(f"💥 Fatal error: {e}")

# Python entry point — only executes when script is run directly
if __name__ == "__main__":
    main()  # Call main() to begin execution
