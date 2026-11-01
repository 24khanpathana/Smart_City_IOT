"""Sensor health. Status rules (minutes since last reading, relative to a reference time):
  unknown  : sensor has no last_seen value
  active   : age <= SENSOR_ACTIVE_MINUTES
  delayed  : SENSOR_ACTIVE_MINUTES < age <= SENSOR_INACTIVE_MINUTES
  inactive : age >  SENSOR_INACTIVE_MINUTES
A silent sensor is NOT automatically faulty (network gaps, maintenance, low-power schedules).
Reference time: HEALTH_REFERENCE=data -> newest last_seen in the dataset; =now -> current clock."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.read_from_cassandra import FMT
from config import get_settings


def _expected_missing(readings: DataFrame) -> DataFrame:
    isn = lambda c: F.col(c).isNull().cast("int")
    cells = (F.when(F.col("sensor_type") == "traffic", F.lit(2))
             .when(F.col("sensor_type") == "environment", F.lit(3)).otherwise(F.lit(1)))
    miss = (F.when(F.col("sensor_type") == "traffic", isn("vehicle_count") + isn("average_speed"))
            .when(F.col("sensor_type") == "environment", isn("temperature") + isn("humidity") + isn("noise_level"))
            .when(F.col("sensor_type") == "parking", isn("parking_occupancy"))
            .otherwise(isn("waste_fill_percentage")))
    return (readings.groupBy("sensor_id").agg(F.count("*").alias("readings_in_window"),
                                              F.sum(miss).alias("missing_cells"), F.sum(cells).alias("expected_cells"))
            .withColumn("missing_fraction", F.col("missing_cells") / F.col("expected_cells")))


def analyze(spark: SparkSession, status: DataFrame, readings: DataFrame, rejects_path: str,
            active_min: int, inactive_min: int, reference: str = "data") -> dict[str, DataFrame]:
    ref = (status.agg(F.max("last_seen").alias("ref")) if reference == "data"
           else spark.range(1).select(F.current_timestamp().alias("ref")))
    s = status.crossJoin(ref).withColumn(
        "age_minutes", (F.unix_timestamp("ref") - F.unix_timestamp("last_seen")) / 60.0)
    s = s.withColumn("status", F.when(F.col("last_seen").isNull(), "unknown")
                     .when(F.col("age_minutes") <= active_min, "active")
                     .when(F.col("age_minutes") <= inactive_min, "delayed").otherwise("inactive"))

    try:  # invalid-record counts come from the ingestion reject log
        rej = (spark.read.csv(rejects_path, header=True).filter(F.col("sensor_id").isNotNull())
               .groupBy("sensor_id").agg(F.count("*").alias("invalid_records")))
    except Exception:  # file absent -> no rejected records known
        rej = spark.createDataFrame([], StructType([StructField("sensor_id", StringType()),
                                                    StructField("invalid_records", StringType())]))
        rej = rej.withColumn("invalid_records", F.col("invalid_records").cast("long"))

    health = (s.join(_expected_missing(readings), "sensor_id", "left").join(rej, "sensor_id", "left")
              .na.fill({"invalid_records": 0, "readings_in_window": 0})
              .select("sensor_id", "location_id", "sensor_type", F.col("last_seen"), "age_minutes", "status",
                      "readings_in_window", "missing_fraction", "invalid_records", "latitude", "longitude"))
    summary = health.groupBy("status").count()
    locations = health.groupBy("location_id").agg(F.avg("latitude").alias("latitude"),
                                                  F.avg("longitude").alias("longitude"),
                                                  F.countDistinct("sensor_id").alias("sensors"))
    return {"sensor_health": health, "sensor_health_summary": summary, "locations": locations}


def persist_status(health: DataFrame) -> None:
    """Write computed status back to latest_sensor_status (only sensor_id + status columns)."""
    (health.select("sensor_id", "status").write.format(FMT).mode("append")
     .options(table="latest_sensor_status", keyspace=get_settings().keyspace).save())
