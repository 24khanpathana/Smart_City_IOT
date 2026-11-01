"""Waste-bin analytics. Thresholds are OPERATIONAL settings (env vars), not universal standards."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def analyze(readings: DataFrame, warning_pct: float = 80.0, critical_pct: float = 90.0) -> dict[str, DataFrame]:
    w_all = readings.filter((F.col("sensor_type") == "waste") & F.col("waste_fill_percentage").isNotNull())

    w = Window.partitionBy("sensor_id").orderBy(F.desc("ts"))
    latest = (w_all.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
              .withColumn("attention", F.when(F.col("waste_fill_percentage") >= critical_pct, "critical")
                          .when(F.col("waste_fill_percentage") >= warning_pct, "warning").otherwise("ok"))
              .select("sensor_id", "location_id", "ts", F.col("waste_fill_percentage").alias("fill_pct"), "attention"))
    counts = w_all.groupBy("sensor_id").agg(F.count("*").alias("history_readings"))
    latest = latest.join(counts, "sensor_id")

    by_location = w_all.groupBy("location_id").agg(F.avg("waste_fill_percentage").alias("avg_fill"),
                                                   F.max("waste_fill_percentage").alias("max_fill"),
                                                   F.countDistinct("sensor_id").alias("bins"))
    hourly = w_all.groupBy("location_id", "sensor_id", "hour_start", "day").agg(
        F.avg("waste_fill_percentage").alias("avg_fill"), F.count("*").alias("readings"))
    needs_action = latest.filter(F.col("attention") != "ok").orderBy(F.desc("fill_pct"))
    # NOTE: time-until-full prediction is intentionally not implemented (bins are emptied irregularly and the
    # history is short/synthetic, so any estimate would not be reliable).
    return {"waste_latest": latest, "waste_by_location": by_location, "waste_hourly": hourly,
            "waste_needs_action": needs_action}
