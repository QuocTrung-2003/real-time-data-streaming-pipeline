from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, window, to_timestamp,
    when
)
from pyspark.sql.functions import first
from consumer import read_kafka_stream
from transformer import parse_event
from validator import validate
from processing_service.config import CHECKPOINT_PATH





# =========================
# MAIN PIPELINE
# =========================
def main():

    spark = SparkSession.builder \
        .appName("WeatherProcessingService") \
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.postgresql:postgresql:42.7.3"
            ])
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Kafka stream
    df = read_kafka_stream(spark)

    # 2. Parse JSON
    parsed_df = parse_event(df)


    # 3. Validate
    # validated_df = validate(parsed_df)
    validated_df = parsed_df
    
    # 4. Clean nulls
    cleaned_df = validated_df.filter(
        col("time").isNotNull() &
        col("temperature").isNotNull() &
        col("windspeed").isNotNull()
    )

    # 5. Event time FIXED
    enriched_df = cleaned_df.withColumn(
        "event_time",
        to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm")
    ).filter(col("event_time").isNotNull())


    # =========================
    # 9. DEBUG CONSOLE
    # =========================
    console_query = aggregated_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", CHECKPOINT_PATH + "/console") \
        .trigger(processingTime="20 seconds") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()