import time
from loguru import logger
from ingestion_service.config import TOPIC, INTERVAL
from api_client import fetch_data
from producer import create_producer

def main():
    producer = create_producer()
    logger.info("Starting ingestion service...")

    while True:
        data = fetch_data()
        if data:
            producer.send(TOPIC, value=data)
            logger.info(f"Sent: \
                         {data['time']}, \
                         {data['interval']}, \
                         {data['winddirection']}, \
                         {data['is_day']}, \
                         {data['weathercode']}, \
                         {data['temperature']}, \
                         {data['windspeed']}" )

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()