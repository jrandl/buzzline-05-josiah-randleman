"""
consumer_josiahrandleman.py
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys

from kafka import KafkaConsumer

# Import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Correct import
from consumers.db_sqlite_josiahrandleman import init_db, insert_sentiment  

#####################################
# Function to process a single message
#####################################

def process_message(message: dict) -> dict:
    """
    Process a single JSON message for sentiment analysis.
    """
    logger.info(f"Processing message: {message}")

    try:
        category = message.get("category", "unknown")
        author = message.get("author", "anonymous")
        sentiment = float(message.get("sentiment", 0.0))
        timestamp = message.get("timestamp")

        processed_data = {
            "category": category,
            "author": author,
            "sentiment": sentiment,
            "timestamp": timestamp
        }
        logger.info(f"Processed sentiment data: {processed_data}")
        return processed_data

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

#####################################
# Consume Messages from Kafka Topic
#####################################

def consume_messages_from_kafka(topic, kafka_url, group, sql_path, interval_secs):
    """
    Consume messages from Kafka and store sentiment analysis data.
    """
    logger.info("Starting Kafka Consumer for sentiment analysis.")

    try:
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info(f"Consuming messages from topic: {topic}")

    for message in consumer:
        processed_data = process_message(message.value)
        if processed_data:
            insert_sentiment(
                sql_path,
                processed_data["category"],
                processed_data["author"],
                processed_data["sentiment"],
                processed_data["timestamp"]
            )

#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.
    """
    logger.info("Starting Consumer...")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()
        sqlite_path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("Initializing database.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("Starting message consumption.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()
