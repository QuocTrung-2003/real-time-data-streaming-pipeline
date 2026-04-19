import json
from kafka import KafkaProducer
from ingestion_service.config import KAFKA_SERVER

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )