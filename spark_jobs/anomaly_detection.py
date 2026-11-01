"""Z-score anomaly detection in PySpark.   Z = (x - mean) / std

* Statistics are computed per (sensor_id, metric) and - by default - per 6-hour day-part, so a rush-hour
  traffic peak is compared with other rush-hour readings, not with the 3 a.m. baseline.
  Different sensors and sensor types never share statistics.
* The baseline window is the date range being analysed (bounded read).
* Groups with fewer than MIN_OBSERVATIONS or std == 0 are skipped (counted in the summary), never divided by 0.
* Results are POTENTIAL anomalies (statistical outliers) - not confirmed incidents.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.read_from_cassandra import FMT
from config import get_settings

METRICS = {"traffic": ["vehicle_count", "average_speed"],
           "environment": ["temperature", "humidity", "noise_level"],
           "parking": ["parking_occupancy"], "waste": ["waste_fill_percentage"]}


def to_long(readings: DataFrame) -> DataFrame:
    parts = [readings.filter((F.col("sensor_type") == st) & F.col(c).isNotNull())
             .select("sensor_id", "location_id", "sensor_type", "ts", "day", "daypart",
                     F.lit(c).alias("metric"), F.col(c).cast("double").alias("value"))
             for st, cols in METRICS.items() for c in cols]
    return reduce(DataFrame.unionByName, parts)


def detect(readings: DataFrame, threshold: float = 3.0, min_obs: int = 30,
           baseline: str = "daypart") -> tuple[DataFrame, DataFrame]:
    """Returns (anomalies, summary)."""
    if baseline not in ("sensor", "daypart"):
        raise ValueError("baseline must be 'sensor' or 'daypart'")
    long = to_long(readings)
    keys = ["sensor_id", "metric"] + (["daypart"] if baseline == "daypart" else [])
    stats = long.groupBy(*keys).agg(F.count("*").alias("n"), F.avg("value").alias("mean"),
                                    F.stddev_samp("value").alias("std"))
    usable = (F.col("n") >= min_obs) & F.col("std").isNotNull() & (F.col("std") > 0)
    summary = stats.agg(F.count("*").alias("groups"),
                        F.sum((F.col("n") < min_obs).cast("int")).alias("skipped_insufficient_history"),
                        F.sum(((F.col("n") >= min_obs) & (F.coalesce(F.col("std"), F.lit(0.0)) == 0)).cast("int"))
                        .alias("skipped_zero_std"))
    scored = (long.join(stats, keys).filter(usable)
              .withColumn("z_score", (F.col("value") - F.col("mean")) / F.col("std"))
              .filter(F.abs(F.col("z_score")) >= threshold))
    method = f"zscore_{baseline}_t{threshold:g}"
    lo, hi = F.col("mean") - threshold * F.col("std"), F.col("mean") + threshold * F.col("std")
    az = F.abs(F.col("z_score"))
    anomalies = (scored.withColumn("detection_method", F.lit(method))
                 .withColumn("anomaly_id", F.md5(F.concat_ws("|", "sensor_id", "metric", F.col("ts").cast("string"),
                                                              "detection_method")))
                 .withColumn("expected_low", lo).withColumn("expected_high", hi)
                 .withColumn("expected_range", F.concat(F.round(lo, 2).cast("string"), F.lit(" to "),
                                                        F.round(hi, 2).cast("string")))
                 .withColumn("severity", F.when(az >= 5, "critical").when(az >= 4, "high").otherwise("medium"))
                 .withColumn("detected_at", F.current_timestamp())
                 .select("day", "ts", "anomaly_id", "sensor_id", "location_id", "sensor_type", "metric",
                         F.col("value").alias("observed_value"), "expected_low", "expected_high",
                         "expected_range", F.round("z_score", 3).alias("z_score"), "detection_method",
                         "severity", "detected_at"))
    return anomalies, summary


def persist(anomalies: DataFrame) -> None:
    """Upsert into Cassandra. 'status' is deliberately not written so reviewed anomalies keep their review state."""
    (anomalies.write.format(FMT).mode("append")
     .options(table="detected_anomalies", keyspace=get_settings().keyspace).save())
