import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
UTC = timezone.utc
from typing import Optional
from io import StringIO

import pandas as pd
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------- Load Environment Variables ----------------
load_dotenv()

# ---------------- Configure Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------- Kafka Configuration ----------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", "weather-batch")

# ---------------- Visual Crossing API ----------------
VISUAL_CROSSING_API_KEY = "Z36E48JEYXR725237B66F8SFY"   # working key
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

# ---------------- Cities to Collect Historical Weather ----------------
CITIES = [
    "Tampa,FL,USA",
    "Miami,FL,USA",
    "Orlando,FL,USA",
    "Jacksonville,FL,USA",
    "Tallahassee,FL,USA"
]

# ======================================================================
#                         CLASS DEFINITION
# ======================================================================
class HistoricalDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH):
        self.topic = topic
        conf = {"bootstrap.servers": bootstrap_servers, "client.id": "historical-weather-producer-0"}

        try:
            self.producer = Producer(conf)
            logger.info(f"✅ Producer initialized → {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka Producer: {e}")
            raise

    # ------------------------------------------------------------------
    def fetch_historical_data(
        self, city: str, start_date: str, end_date: str, include: str = "days"
    ) -> Optional[pd.DataFrame]:
        """
        Fetch weekly historical weather data for a city using Visual Crossing API (CSV format).
        Docs: https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/
        """
        logger.info(f"🌦️ Fetching {include}-level data for {city} ({start_date} → {end_date})")

        url = (
            f"{BASE_URL}/{city}/{start_date}/{end_date}"
            f"?key={VISUAL_CROSSING_API_KEY}&unitGroup=metric"
            f"&include={include}&contentType=csv"
        )
        headers = {"User-Agent": "WeatherDataCollector/1.0"}

        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers, timeout=60)
                if response.status_code == 429:
                    wait_time = 10 * (attempt + 1)
                    logger.warning(f"⏳ Rate limit for {city}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < 2:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"⚠️ API error for {city}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Failed after 3 attempts for {city}: {e}")
                    return None

        df = pd.read_csv(StringIO(response.text))
        if df.empty:
            logger.warning(f"⚠️ No data returned for {city}")
            return None

        df.columns = [c.strip().lower() for c in df.columns]
        df.rename(
            columns={
                "datetime": "timestamp",
                "temp": "temperature",
                "windspeed": "wind_speed",
                "precip": "precip_mm",
            },
            inplace=True,
        )
        df["city"] = city
        df["date"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")

        logger.info(f"✅ Retrieved {len(df)} records for {city}")
        return df

    # ------------------------------------------------------------------
    def delivery_report(self, err, msg):
        if err:
            logger.error(f"❌ Delivery failed: {err}")
        else:
            logger.debug(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

    # ------------------------------------------------------------------
    def produce_to_kafka(self, df: pd.DataFrame, city: str):
        batch_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        df["batch_id"] = batch_id
        df["batch_date"] = datetime.now(UTC).strftime("%Y-%m-%d")

        success, fail = 0, 0
        for record in df.to_dict(orient="records"):
            try:
                message = json.dumps(record)
                self.producer.produce(
                    topic=self.topic,
                    key=city.encode(),
                    value=message.encode(),
                    callback=self.delivery_report,
                )
                self.producer.poll(0)
                success += 1
            except Exception as e:
                logger.error(f"❌ Failed to send record for {city}: {e}")
                fail += 1

        self.producer.flush()
        logger.info(f"📦 Produced {success} records for {city} (failed {fail})")

    # ------------------------------------------------------------------
    def collect_historical_data(self, start_date: str, end_date: str):
        logger.info(f"🚀 Collecting weekly weather data for {len(CITIES)} cities")

        success, fail = 0, 0
        for city in CITIES:
            df = self.fetch_historical_data(city, start_date, end_date)
            if df is not None and not df.empty:
                self.produce_to_kafka(df, city)
                success += 1
            else:
                fail += 1
            time.sleep(5)  # avoid hitting API limits

        logger.info(f"🏁 Weekly collection complete — Success={success}, Fail={fail}")

# ======================================================================
#                            MAIN FUNCTION
# ======================================================================
def main():
    try:
        logger.info("Starting Weekly Weather Data Collector (CSV API version)")

        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH
        )

        # ----- One week observation window -----
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
        start_date = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")

        collector.collect_historical_data(start_date=start_date, end_date=end_date)

    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")


if __name__ == "__main__":
    main()
