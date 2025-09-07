"""
csv_consumer_meyer.py

Custom Kafka CSV consumer monitoring manufacturing sensor readings.
"""

import json
from collections import deque
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Keep last 5 readings to detect "stall" in temperature
last_readings = deque(maxlen=5)

def process_message(message: str):
    """Process a single sensor reading message."""
    try:
        reading = json.loads(message)
        logger.info(f"Received sensor reading: {reading}")

        last_readings.append(reading["temperature"])

        # Detect stall if temp variation < 0.5 in last 5 readings
        if len(last_readings) == 5 and max(last_readings) - min(last_readings) < 0.5:
            logger.warning(f"Temperature stall detected for machine {reading['machine_id']}! Recent readings: {list(last_readings)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing sensor message: {e}")

def main():
    topic = "manufacturing_csv_topic"
    group_id = "manufacturing_csv_group"

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
        logger.warning("CSV Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka CSV consumer closed.")

if __name__ == "__main__":
    main()
