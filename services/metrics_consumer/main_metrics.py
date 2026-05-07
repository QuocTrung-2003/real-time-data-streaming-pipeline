from kafka import KafkaConsumer
import json
import time
import logging
import os

TOPIC = os.getenv("STREAM_RAW_TOPIC")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
GROUP_ID = os.getenv("STREAM_CONSUMER_GROUP")

logging.basicConfig(level=logging.INFO)

# Retry connection
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        logging.info("Connected to Kafka!")
        break
    except Exception as e:
        logging.warning(f"Kafka not ready, retrying... {e}")
        time.sleep(3)

print("Waiting for messages...")

# Consume messages
has_message = False

for message in consumer:
    has_message = True
    try:
        data = message.value
        print(f"Received: {data}")
        print(f"Partition: {message.partition}, Offset: {message.offset}")

    except Exception as e:
        logging.error(f"Error processing message: {e}")
