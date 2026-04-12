#!/usr/bin/env python3  # Tells the system to run this script using Python 3
# -*- coding: utf-8 -*-  # Ensures the file is interpreted with UTF-8 encoding (handles emojis, symbols, etc.)

"""
Realtime Weather Data Producer
------------------------------
Continuously fetches live weather data from the OpenWeather API
for multiple cities and streams it to a Kafka topic.
"""  # A module-level docstring explaining the script’s overall purpose

# ==================== Standard Library Imports ====================
import json            # Converts Python dictionaries to JSON strings for Kafka messages
import logging         # Provides logging capability for info, warning, and error messages
import os              # Interacts with environment variables and the operating system
import time            # Provides sleep() for pausing between fetch cycles
from datetime import datetime  # Used for timestamps in messages and logs
from typing import Dict        # Adds type hints for clarity and readability

# ==================== Third-Party Library Imports ====================
import requests                    # Used to make HTTP GET requests to the OpenWeather API
from confluent_kafka import Producer  # Kafka producer from Confluent’s Python client
from dotenv import load_dotenv         # Loads key/value pairs from a .env file into environment variables

# ============================================================
# Load environment variables
# ============================================================
load_dotenv()  
# Reads the .env file in the project directory and loads its contents into environment variables.
# This allows credentials and configs (like API keys, broker URLs) to be stored securely outside the code.

# ============================================================
# Logging Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,                         # Sets global logging level (INFO and above are shown)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Defines log output format (timestamp, level, message)
)
logger = logging.getLogger(__name__)            # Creates a logger instance for this module for structured logging

# ============================================================
# Kafka Configuration
# ============================================================
# When running inside Docker, Kafka hostname is often 'kafka:9092'
# When running locally, it’s 'localhost:29092'
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "<BROKER_URL>")
# Loads the Kafka broker address from environment variables (with a safe default placeholder)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_REALTIME", "weather-realtime")
# Loads Kafka topic name from environment variables or defaults to 'weather-realtime'

# ============================================================
# API and Cities Configuration
# ============================================================
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "<SECRET>")
# Reads the API key for OpenWeather API from .env file; required for authentication

if not OPENWEATHER_API_KEY:
    raise EnvironmentError("❌ Missing OPENWEATHER_API_KEY in .env file")
# If the key is not found, raise an error immediately so the program doesn’t run without credentials

CITIES = [c.strip() for c in os.getenv(
    "CITIES", "Tampa,Miami,Orlando,Jacksonville,Tallahassee"
).split(",") if c.strip()]
# Reads the CITIES environment variable and splits it by commas into a list.
# If it’s not found, defaults to five major Florida cities.
# `.strip()` cleans extra spaces; list comprehension filters out empty entries.

# ============================================================
# Class Definition: StreamDataCollector
# ============================================================
class StreamDataCollector:
    """
    Continuously collects weather data from the OpenWeather API
    and streams it to Kafka.
    """  # A class-level docstring describing what the class is responsible for

    # ----------------------------------------------------------------
    # Constructor
    # ----------------------------------------------------------------
    def __init__(self,
                 bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 topic: str = KAFKA_TOPIC,
                 interval: int = 60):
        # Initialize class attributes for configuration
        self.logger = logging.getLogger(__name__)  # Local logger for class methods
        self.topic = topic                         # Kafka topic name
        self.interval = interval                   # Time delay between fetch cycles (in seconds)
        self.api_key = OPENWEATHER_API_KEY         # API key for weather requests
        self.cities = CITIES                       # List of target cities

        # Kafka producer configuration dictionary
        self.producer_conf = {
            "bootstrap.servers": bootstrap_servers,              # Kafka broker address
            "client.id": "continuous-weather-data-producer"      # Identifies the producer client in Kafka logs
        }

        # Attempt to create the Kafka producer
        try:
            self.producer = Producer(self.producer_conf)  # Create a Producer instance using Confluent’s API
            self.logger.info(f"✅ Producer initialized. Sending to: {bootstrap_servers}, Topic: {topic}")
        except Exception as e:
            # Logs and re-raises the exception if Kafka producer creation fails
            self.logger.error(f"❌ Failed to create Kafka Producer: {e}")
            raise

    # ========================================================
    # Delivery Report Callback
    # ========================================================
    def delivery_report(self, err, msg):
        """
        Kafka callback executed once the message is delivered (or failed).
        Used to verify successful delivery and debug if needed.
        """
        if err is not None:
            # Logs an error if the message could not be delivered
            self.logger.error(f"❌ Delivery failed for message: {msg.key()}: {err}")
        else:
            # Logs a debug-level success message with topic, partition, and offset
            self.logger.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    # ========================================================
    # Fetch Weather Data from API
    # ========================================================
    def fetch_weather_data(self, city: str) -> Dict:
        """
        Fetch real-time weather data from the OpenWeather API for a given city.
        Returns a dictionary with weather metrics.
        """
        # Construct API endpoint with city, API key, and metric units (Celsius)
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.api_key}&units=metric"
        try:
            response = requests.get(url, timeout=10)  # Sends GET request with a 10-second timeout
            response.raise_for_status()               # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()                    # Parses the API response JSON to a Python dict

            # Extracts required weather attributes from the response
            weather_data = {
                "city": city,                                     # City name
                "temperature": data["main"]["temp"],              # Current temperature
                "humidity": data["main"]["humidity"],             # Relative humidity %
                "pressure": data["main"]["pressure"],             # Atmospheric pressure in hPa
                "weather": data["weather"][0]["main"],            # Main weather condition (e.g., Clear, Rain)
                "wind_speed": data.get("wind", {}).get("speed"),  # Wind speed if available
                "timestamp": datetime.utcnow().isoformat()        # UTC timestamp when fetched
            }

            return weather_data  # Returns dictionary to the caller

        except Exception as e:
            # Logs any network or parsing errors
            self.logger.error(f"⚠️ Error fetching weather for {city}: {e}")
            return {}  # Returns empty dict to signal failure

    # ========================================================
    # Continuous Data Production
    # ========================================================
    def produce_weather_data(self):
        """
        Main loop that continuously fetches and publishes weather data
        for all configured cities to the Kafka topic.
        """
        self.logger.info("🌍 Starting continuous weather data production")  # Start banner log

        try:
            while True:  # Infinite loop that keeps running until interrupted
                run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")  # Unique run identifier (timestamp)
                self.logger.info(f"🌦️ Starting new cycle (run_id={run_id}) for {len(self.cities)} cities")

                successful, failed = 0, 0  # Counters for success and failure metrics

                # Iterate over each city configured in CITIES
                for city in self.cities:
                    try:
                        weather_data = self.fetch_weather_data(city)  # Call the API for each city
                        if weather_data:  # Proceed only if data was successfully fetched
                            weather_data["run_id"] = run_id           # Tag data with run ID for traceability
                            message = json.dumps(weather_data)        # Convert dictionary to JSON string

                            # Send message asynchronously to Kafka
                            self.producer.produce(
                                self.topic,                           # Target Kafka topic
                                key=city.encode("utf-8"),             # Message key (helps Kafka partitioning)
                                value=message.encode("utf-8"),        # Message body as UTF-8 encoded JSON
                                callback=self.delivery_report          # Callback for confirmation
                            )
                            self.producer.poll(0)                     # Triggers delivery callback events
                            successful += 1                           # Increment success counter
                        else:
                            failed += 1                               # If empty dict, count as failure

                    except Exception as e:
                        # Catches errors in message preparation or production
                        self.logger.error(f"❌ Error processing {city}: {e}")
                        failed += 1

                # After all cities processed, log summary for that cycle
                self.logger.info(f"✅ Summary: Success={successful}, Failed={failed}")
                self.producer.flush()  # Ensures all queued messages are delivered before next cycle

                # Wait before fetching again
                self.logger.info(f"⏳ Waiting {self.interval} seconds before next weather fetch...")
                time.sleep(self.interval)  # Sleep to respect API limits and avoid flooding Kafka

        except KeyboardInterrupt:
            # Graceful exit when user stops the script manually (Ctrl+C)
            self.logger.info("🛑 Producer stopped by user.")
        except Exception as e:
            # Logs unexpected runtime errors
            self.logger.error(f"❌ Unexpected error: {e}")
        finally:
            # Ensures producer shuts down cleanly even if an exception occurs
            self.logger.info("🔒 Shutting down producer.")
            self.producer.flush()  # Final flush of pending messages

# ============================================================
# Main Entry Point
# ============================================================
def main():
    """
    Entry point that initializes the StreamDataCollector and
    starts the continuous data production loop.
    """
    try:
        # Instantiate StreamDataCollector with values loaded from environment
        producer = StreamDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # Kafka brokers
            topic=KAFKA_TOPIC,                          # Topic name
            interval=int(os.getenv("FETCH_INTERVAL", "60"))  # Time between fetches (default 60s)
        )
        producer.produce_weather_data()  # Start streaming data to Kafka
    except Exception as e:
        # Catch and log fatal initialization errors
        logger.error(f"💥 Fatal error: {e}")

# Python idiom to ensure code runs only when file is executed directly, not imported
if __name__ == "__main__":
    main()  # Call the main() function to start the producer
