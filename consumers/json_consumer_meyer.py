"""
json_consumer_meyer.py

Custom Kafka JSON consumer for manufacturing machine status alerts.
"""

import json
from collections import defaultdict
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
import os

# Track machine statuses
machine_status_counts = defaultdict(int)


def process_message(message: str):
    """Process a single JSON message from Kafka."""
    try:
        message_dict = json.loads(message)
        logger.info(f"Received message: {message_dict}")

        # Track machine status
        status = message_dict.get("status", "unknown")
        machine_status_counts[status] += 1
        logger.info(f"Updated status counts: {dict(machine_status_counts)}")

        # Alert if machine has error
        if status.lower() == "error":
            logger.warning(f"ALERT: Machine {message_dict.get('machine_id')} reported error {message_dict.get('error_code')}!")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            # poll returns a dict: {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    # value_deserializer in utils_consumer already decoded this to str
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        
    logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
