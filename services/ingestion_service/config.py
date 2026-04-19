import os

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC = os.getenv("STREAM_RAW_TOPIC", "weather.raw")

LAT = float(os.getenv("LAT", 10.8231))
LON = float(os.getenv("LON", 106.6297))
INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", 60))


