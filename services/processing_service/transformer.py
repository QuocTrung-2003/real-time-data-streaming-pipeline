from processing_service.schema import event_schema
from pyspark.sql.functions import from_json, col

# def parse_event(df):
#     return df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), event_schema).alias("data")) \
#         .select("data.*")

def parse_event(df):
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), event_schema).alias("data")) \
        .select("data.*")

    return parsed_df