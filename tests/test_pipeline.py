from pyspark.sql import SparkSession
from services.processing_service.transformer import parse_event


# =========================
# TEST SETUP
# =========================
def setup_spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test-pipeline") \
        .getOrCreate()


# =========================
# 1. TEST PARSE EVENT
# =========================
def test_parse_event():
    spark = setup_spark()

    data = [('{"time": "2026-04-18T10:00", "interval": 900, "temperature": 30.5, "windspeed": 10.2, "winddirection": 120, "is_day": 1, "weathercode": 2}',)]
    df = spark.createDataFrame(data, ["value"])

    result_df = parse_event(df)

    # row exists
    assert result_df.count() == 1

    row = result_df.collect()[0]

    # value correctness
    assert row.temperature == 30.5
    assert row.windspeed == 10.2
    assert row.winddirection == 120
    assert row.is_day == 1
    assert row.weathercode == 2

    # schema correctness
    expected_cols = [
        "time", "interval", "temperature",
        "windspeed", "winddirection",
        "is_day", "weathercode"
    ]

    for col in expected_cols:
        assert col in result_df.columns


# =========================
# 2. TEST DATA VALIDATION RULES
# =========================
def test_data_constraints():
    spark = setup_spark()

    data = [('{"time": "2026-04-18T10:00", "interval": 900, "temperature": 30.5, "windspeed": 10.2, "winddirection": 120, "is_day": 1, "weathercode": 2}',)]
    df = spark.createDataFrame(data, ["value"])

    result_df = parse_event(df)
    row = result_df.collect()[0]

    # range checks
    assert -80 <= row.temperature <= 60
    assert 0 <= row.windspeed <= 200
    assert 0 <= row.winddirection <= 360

    # logical checks
    assert row.is_day in [0, 1]
    assert row.weathercode >= 0


# =========================
# 3. TEST INVALID INPUT (ROBUSTNESS)
# =========================
def test_invalid_json():
    spark = setup_spark()

    data = [("invalid-json",)]
    df = spark.createDataFrame(data, ["value"])

    result_df = parse_event(df)

    # should not crash
    assert result_df is not None
    assert result_df.count() == 0


# =========================
# 4. TEST MISSING FIELD SAFETY
# =========================
def test_missing_fields():
    spark = setup_spark()

    data = [('{"temperature": 30}',)]
    df = spark.createDataFrame(data, ["value"])

    result_df = parse_event(df)

    row = result_df.collect()[0]

    # missing fields should be null (not crash)
    assert row.temperature == 30