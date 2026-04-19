from pyspark.sql.functions import col

def validate(df):
    return df.filter(
        # NULL checks
        col("temperature").isNotNull() &
        col("windspeed").isNotNull() &
        col("time").isNotNull()
    )