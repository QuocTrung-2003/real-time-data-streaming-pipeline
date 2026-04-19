from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, window, to_timestamp,
    when
)
from pyspark.sql.functions import first , from_utc_timestamp
from consumer import read_kafka_stream
from transformer import parse_event
from validator import validate
from processing_service.config import CHECKPOINT_PATH


# =========================
# WRITE TO POSTGRES
# =========================
def write_to_postgres(batch_df, batch_id):

    print(f"Batch ID: {batch_id}")
    batch_df.show(truncate=False)

    batch_df = batch_df.select(
        col("window").getField("start").alias("window_start"),
        col("window").getField("end").alias("window_end"),
        "avg_temperature",
        "avg_windspeed",
        "max_temperature",
        "max_windspeed",
        "wind_level",
        "day_night",
        "weather_category"
    )

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "stream_aggregates") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



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
         from_utc_timestamp(
            to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"),
            "Asia/Ho_Chi_Minh"
         )
    ).filter(col("event_time").isNotNull())

    # =========================
    # 6. FEATURE ENGINEERING
    # =========================
    enriched_df = enriched_df \
        .withColumn(
            "wind_level",
            when(col("windspeed") < 5, "low")
            .when(col("windspeed") < 15, "medium")
            .otherwise("high")
        ) \
        .withColumn(
            "day_night",
            when(col("is_day") == 1, "day").otherwise("night")
        ) \
        .withColumn(
            "weather_category",
            when(col("weathercode").isin([0]), "clear")          # 0 = trời quang
            .when(col("weathercode").isin([1,2,3]), "cloudy")    # 1,2,3 = mây thưa, ít, nhiều
            .when(col("weathercode").isin([45,48]), "fog")       # 45,48 = sương mù, sương mù đóng băng
            .when(col("weathercode").isin([51,53,55]), "drizzle")# 51,53,55 = mưa phùn nhẹ, vừa, nặng
            .when(col("weathercode").isin([61,63,65]), "rain")   # 61,63,65 = mưa nhẹ, vừa, nặng
            .otherwise("unknown")

        )

    # =========================
    # 7. AGGREGATION
    # =========================
    aggregated_df = enriched_df \
        .groupBy(window(col("event_time"), "5 minutes")) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("windspeed").alias("avg_windspeed"),
            max("temperature").alias("max_temperature"),
            max("windspeed").alias("max_windspeed"),
            first("wind_level", True).alias("wind_level"),
            first("day_night", True).alias("day_night"),
            first("weather_category", True).alias("weather_category")
        )

    # =========================
    # 8. POSTGRES SINK
    # =========================
    db_query = aggregated_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_PATH + "/db") \
        .trigger(processingTime="20 seconds") \
        .start()

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