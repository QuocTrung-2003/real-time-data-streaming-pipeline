from pyspark.sql.types import *

event_schema = StructType([
    StructField("time", StringType(), True),
    StructField("interval", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("winddirection", DoubleType(), True),
    StructField("is_day", IntegerType(), True),
    StructField("weathercode", IntegerType(), True),
])
#định nghĩa cấu trúc dữ liệu đầu vào/đầu ra