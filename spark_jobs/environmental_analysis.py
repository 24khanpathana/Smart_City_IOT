"""Environmental analytics (temperature, humidity, noise). PM2.5/PM10 are not part of this data model."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def analyze(readings: DataFrame, max_rows: int = 5000) -> dict[str, DataFrame]:
    e = readings.filter(F.col("sensor_type") == "environment")

    by_location = e.groupBy("location_id").agg(
        F.avg("temperature").alias("avg_temperature"), F.max("temperature").alias("max_temperature"),
        F.min("temperature").alias("min_temperature"), F.avg("humidity").alias("avg_humidity"),
        F.avg("noise_level").alias("avg_noise"), F.max("noise_level").alias("max_noise"),
        F.count("*").alias("readings"))

    hourly = e.groupBy("location_id", "hour_start", "day", "hour").agg(
        F.avg("temperature").alias("avg_temperature"), F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"), F.avg("humidity").alias("avg_humidity"),
        F.avg("noise_level").alias("avg_noise"), F.max("noise_level").alias("max_noise"),
        F.count("*").alias("readings"))

    humidity_dist = (e.filter(F.col("humidity").isNotNull())
                     .withColumn("humidity_bin", (F.floor(F.col("humidity") / 10) * 10).cast("int"))
                     .groupBy("location_id", "day", "humidity_bin").count())

    # Statistical outliers vs. the location's own 95th percentile. NOT regulatory limit violations.
    p95 = e.groupBy("location_id").agg(F.percentile_approx("temperature", 0.95).alias("temp_p95"),
                                       F.percentile_approx("noise_level", 0.95).alias("noise_p95"))
    high = (e.join(p95, "location_id")
            .filter((F.col("temperature") > F.col("temp_p95")) | (F.col("noise_level") > F.col("noise_p95")))
            .select("location_id", "sensor_id", "ts", "day", "temperature", "temp_p95", "noise_level", "noise_p95")
            .orderBy(F.desc("temperature")).limit(max_rows))
    return {"environment_by_location": by_location, "environment_hourly": hourly,
            "environment_humidity_dist": humidity_dist, "environment_high_readings": high}
