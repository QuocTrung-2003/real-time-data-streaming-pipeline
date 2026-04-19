from pyspark.sql import SparkSession
from processing_service.config import KAFKA_SERVER, TOPIC_RAW

def read_kafka_stream(spark: SparkSession):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()