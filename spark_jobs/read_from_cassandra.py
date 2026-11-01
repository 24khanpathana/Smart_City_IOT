"""Reading from Cassandra through the Spark Cassandra Connector + common cleaning steps."""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import METRIC_COLUMNS, VALUE_RANGES, get_settings

FMT = "org.apache.spark.sql.cassandra"


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format(FMT).options(table=table, keyspace=get_settings().keyspace).load()


def read_status(spark: SparkSession) -> DataFrame:
    """Sensor registry (one row per sensor) - a small table, so a full read is acceptable."""
    return read_table(spark, "latest_sensor_status")


def date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end date must not be before start date")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def read_readings(spark: SparkSession, locations: list[str], days: list[date]) -> DataFrame:
    """Bounded read of sensor_readings_by_location.

    Filtering on BOTH partition-key columns (location_id, day) with equality/IN lets the connector
    push the predicate down to Cassandra, so only the requested partitions are read.
    (Check with .explain(): the filters should appear under PushedFilters / Cassandra Scan.)
    """
    if not locations or not days:
        raise ValueError("locations and days must not be empty (unbounded reads are not allowed)")
    return read_table(spark, "sensor_readings_by_location").filter(
        F.col("location_id").isin(locations) & F.col("day").isin(days))


def prepare_readings(df: DataFrame) -> DataFrame:
    """Schema/type normalisation, invalid-value filtering and duplicate removal."""
    d = df.withColumn("ts", F.col("ts").cast("timestamp"))
    for c in METRIC_COLUMNS:
        value = F.col(c).cast("double") if c in d.columns else F.lit(None).cast("double")
        d = d.withColumn(c, value)
    d = d.filter(F.col("ts").isNotNull() & F.col("sensor_id").isNotNull())
    d = d.dropDuplicates(["sensor_id", "ts"])
    # Invalid-value filtering: impossible values become NULL (never silently treated as real data)
    for c, (lo, hi) in VALUE_RANGES.items():
        d = d.withColumn(c, F.when(F.col(c).between(lo, hi), F.col(c)))
    # Safe parking maths: zero / missing capacity gives NULL instead of a division error
    ok = (F.col("parking_capacity") > 0) & F.col("occupied_spaces").isNotNull()
    d = (d.withColumn("parking_occupancy", F.when(ok, F.col("occupied_spaces") / F.col("parking_capacity") * 100))
         .withColumn("available_spaces", F.when(ok, F.greatest(F.col("parking_capacity") - F.col("occupied_spaces"), F.lit(0.0)))))
    return (d.withColumn("hour", F.hour("ts")).withColumn("daypart", (F.hour("ts") / 6).cast("int"))
            .withColumn("hour_start", F.date_trunc("hour", F.col("ts"))))


def missing_value_report(df: DataFrame) -> DataFrame:
    """Rows and NULL counts per sensor type (metrics that do not apply to a type are always NULL)."""
    return df.groupBy("sensor_type").agg(
        F.count("*").alias("rows"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in METRIC_COLUMNS])


def duplicate_report(df: DataFrame) -> int:
    """Number of (sensor_id, ts) keys appearing more than once in the given DataFrame."""
    return df.groupBy("sensor_id", "ts").count().filter(F.col("count") > 1).count()
