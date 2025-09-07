"""
csv_producer_meyer.py

Custom Kafka CSV producer simulating smart manufacturing sensor readings.
"""

import time
from kafka import KafkaProducer
import json
from utils.utils_logger import logger
from utils.utils_producer import verify_services, create_kafka_producer, create_kafka_topic

# Hardcoded CSV-style readings (timestamp, machine_id, temperature)
SENSOR_DATA = [
    {"timestamp": "2025-09-07 08:00:00", "machine_id": "M001", "temperature": 75.2},
    {"timestamp": "2025-09-07 08:01:00", "machine_id": "M001", "temperature": 75.5},
    {"timestamp": "2025-09-07 08:02:00", "machine_id": "M001", "temperature": 75.8},
    {"timestamp": "2025-09-07 08:03:00", "machine_id": "M001", "temperature": 76.0},
]

def generate_messages():
    """Yield CSV messages as dicts to send as JSON to Kafka."""
    while True:
        for row in SENSOR_DATA:
            logger.info(f"Generated CSV message: {row}")
            yield row
            time.sleep(2)

def main():
    topic = "manufacturing_csv_topic"

    verify_services()
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    create_kafka_topic(topic)

    try:
        for message in generate_messages():
            producer.send(topic, value=message)
            logger.info(f"Sent message to topic '{topic}': {message}")
    except KeyboardInterrupt:
        logger.warning("CSV Producer interrupted by user.")
    finally:
        producer.close()
        logger.info("Kafka CSV producer closed.")

if __name__ == "__main__":
    main()
