import os

# =========================
# Kafka config
# =========================
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

TOPIC_RAW = os.getenv("STREAM_RAW_TOPIC")
PROCESSED_TOPIC = os.getenv("STREAM_PROCESSED_TOPIC")
DLQ_TOPIC = os.getenv("STREAM_DLQ_TOPIC")

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "weather-processing-group"
)

# =========================
# Processing config
# =========================
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/tmp/checkpoints/weather_stats"
)

# =========================
# Validation (fail-fast)
# =========================
def validate_config():
    required_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_SERVER,
        "SREAM_RAW_TOPIC": TOPIC_RAW,
        "STREAM_PROCESSED_TOPIC": PROCESSED_TOPIC,
    }

    missing = [k for k, v in required_vars.items() if not v]

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )