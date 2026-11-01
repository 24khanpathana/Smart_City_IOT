"""Parking analytics. occupancy % = occupied / capacity * 100 (NULL when capacity <= 0 or missing)."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def analyze(readings: DataFrame, high_demand_pct: float = 80.0) -> dict[str, DataFrame]:
    p_all = readings.filter(F.col("sensor_type") == "parking")
    p = p_all.filter(F.col("parking_occupancy").isNotNull())

    by_location = (p.groupBy("location_id").agg(
        F.avg("parking_occupancy").alias("avg_occupancy"), F.max("parking_occupancy").alias("max_occupancy"),
        F.avg("parking_capacity").alias("avg_capacity"), F.avg("available_spaces").alias("avg_available"),
        F.count("*").alias("readings"))
        .withColumn("high_demand", F.col("avg_occupancy") >= high_demand_pct))

    hourly = p.groupBy("location_id", "hour_start", "day", "hour").agg(
        F.avg("parking_occupancy").alias("avg_occupancy"), F.max("parking_occupancy").alias("max_occupancy"),
        F.avg("available_spaces").alias("avg_available"), F.avg("parking_capacity").alias("avg_capacity"),
        F.count("*").alias("readings"))

    w = Window.partitionBy("sensor_id").orderBy(F.desc("ts"))
    latest = (p.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
              .select("sensor_id", "location_id", "ts", "parking_capacity", "occupied_spaces",
                      "parking_occupancy", "available_spaces"))

    quality = p_all.agg(F.count("*").alias("parking_readings"),
                        F.sum(F.col("parking_occupancy").isNull().cast("int")).alias("excluded_invalid_or_missing"))
    return {"parking_by_location": by_location, "parking_hourly": hourly, "parking_latest": latest,
            "parking_quality": quality}
