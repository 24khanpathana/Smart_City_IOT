"""PySpark logic on small in-memory DataFrames (needs pyspark + Java, NOT Cassandra)."""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs import anomaly_detection, parking_analysis, sensor_health_analysis, traffic_analysis  # noqa: E402
from spark_jobs.read_from_cassandra import prepare_readings  # noqa: E402

pytestmark = pytest.mark.spark
COLS = ["sensor_id", "location_id", "sensor_type", "ts", "vehicle_count", "average_speed", "temperature", "humidity",
        "noise_level", "parking_capacity", "occupied_spaces", "waste_fill_percentage"]
T0 = datetime(2025, 1, 1, 0, 0)


def frame(spark, rows):
    double_columns = set(COLS[4:])
    full = [tuple(float(r[c]) if c in double_columns and r.get(c) is not None else r.get(c) for c in COLS)
            for r in rows]
    df = spark.createDataFrame(full, "sensor_id string, location_id string, sensor_type string, ts timestamp, "
                                     "vehicle_count double, average_speed double, temperature double, humidity double, "
                                     "noise_level double, parking_capacity double, occupied_spaces double, waste_fill_percentage double")
    return prepare_readings(df.withColumn("day", F.to_date("ts")))


def traffic(i, sensor="T1", loc="L1", count=10, speed=50.0):
    return dict(sensor_id=sensor, location_id=loc, sensor_type="traffic", ts=T0 + timedelta(minutes=15 * i),
                vehicle_count=count, average_speed=speed)


def test_traffic_totals_are_correct(spark):
    df = frame(spark, [traffic(0, count=10), traffic(1, count=30), traffic(2, "T2", "L2", count=5)])
    out = traffic_analysis.analyze(df)["traffic_by_location"].toPandas().set_index("location_id")
    assert out.loc["L1", "total_vehicles"] == 40 and out.loc["L2", "total_vehicles"] == 5
    assert out.loc["L1", "volume_rank"] == 1 and out.loc["L1", "avg_speed"] == 50.0


def test_high_volume_with_normal_speed_is_not_congestion(spark):
    rows = [traffic(i * 4, count=c, speed=s) for i, (c, s) in enumerate([(10, 60), (11, 60), (12, 60), (100, 58), (100, 20)])]
    res = traffic_analysis.analyze(frame(spark, rows))["traffic_congestion"].toPandas().sort_values("hour_start")
    assert "likely congested" in set(res.assessment) and "high volume, flowing" in set(res.assessment)


def test_invalid_parking_capacity_gives_null_not_error(spark):
    rows = [dict(sensor_id="P1", location_id="L1", sensor_type="parking", ts=T0, parking_capacity=0.0, occupied_spaces=5.0),
            dict(sensor_id="P2", location_id="L1", sensor_type="parking", ts=T0, parking_capacity=100.0, occupied_spaces=25.0)]
    out = parking_analysis.analyze(frame(spark, rows))
    assert out["parking_by_location"].first()["avg_occupancy"] == 25.0
    assert out["parking_quality"].first()["excluded_invalid_or_missing"] == 1


def test_zscore_detects_single_outlier(spark):
    rows = [dict(sensor_id="E1", location_id="L1", sensor_type="environment", ts=T0 + timedelta(minutes=i),
                 temperature=20.0 + (i % 3) * 0.1) for i in range(60)]
    rows.append(dict(sensor_id="E1", location_id="L1", sensor_type="environment", ts=T0 + timedelta(minutes=61), temperature=60.0))
    an, summary = anomaly_detection.detect(frame(spark, rows), threshold=3.0, min_obs=30, baseline="sensor")
    got = an.toPandas()
    assert len(got) == 1 and got.iloc[0]["observed_value"] == 60.0 and got.iloc[0]["metric"] == "temperature"


def test_zero_standard_deviation_is_skipped(spark):
    rows = [dict(sensor_id="E1", location_id="L1", sensor_type="environment", ts=T0 + timedelta(minutes=i), temperature=21.0) for i in range(40)]
    an, summary = anomaly_detection.detect(frame(spark, rows), min_obs=30, baseline="sensor")
    assert an.count() == 0 and summary.first()["skipped_zero_std"] == 1


def test_insufficient_history_is_skipped(spark):
    rows = [dict(sensor_id="E1", location_id="L1", sensor_type="environment", ts=T0 + timedelta(minutes=i), temperature=float(i)) for i in range(5)]
    an, summary = anomaly_detection.detect(frame(spark, rows), min_obs=30, baseline="sensor")
    assert an.count() == 0 and summary.first()["skipped_insufficient_history"] >= 1


def test_sensor_status_rules_and_missing_readings(spark):
    now = datetime(2025, 1, 2, 12, 0)
    status = spark.createDataFrame(
        [("A", "L1", "traffic", now, None, None), ("B", "L1", "traffic", now - timedelta(minutes=60), None, None),
         ("C", "L1", "traffic", now - timedelta(days=1), None, None), ("D", "L1", "traffic", None, None, None)],
        "sensor_id string, location_id string, sensor_type string, last_seen timestamp, latitude double, longitude double")
    readings = frame(spark, [traffic(0, "A", count=None), traffic(1, "A", count=5)])  # one missing vehicle_count
    res = sensor_health_analysis.analyze(spark, status, readings, "/nonexistent/rejects.csv", 30, 180)
    h = res["sensor_health"].toPandas().set_index("sensor_id")
    assert [h.loc[s, "status"] for s in "ABCD"] == ["active", "delayed", "inactive", "unknown"]
    assert h.loc["A", "missing_fraction"] == pytest.approx(0.25)  # 1 of 4 expected cells missing
