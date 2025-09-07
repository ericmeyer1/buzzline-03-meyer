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


def main():
    topic = "manufacturing_json_topic"
    group_id = "manufacturing_json_group"

    consumer = create_kafka_consumer(topic, group_id)

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for _, batch in records.items():
                for msg in batch:
                    process_message(msg.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
