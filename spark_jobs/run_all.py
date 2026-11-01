"""Run all PySpark analytics for a bounded date window and publish SMALL aggregated results.

  python -m spark_jobs.run_all                    # last 7 days of data present in Cassandra
  python -m spark_jobs.run_all --start 2025-01-02 --end 2025-01-04
Results -> data/processed/results/*.csv + manifest.json (the dashboard reads these);
anomalies and sensor status are also written back to Cassandra.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType)

import pandas as pd

from config import CSV_COLUMNS, LOCK_FILE, METRIC_COLUMNS, REJECTS_CSV, RESULTS_DIR, get_settings
from ingestion.validate_data import load_csv, validate_dataframe, write_rejects
from spark_jobs import (anomaly_detection, environmental_analysis, parking_analysis, sensor_health_analysis,
                        traffic_analysis, waste_analysis)
from spark_jobs.read_from_cassandra import (date_range, duplicate_report, missing_value_report,
                                            prepare_readings, read_readings, read_status)
from spark_jobs.spark_session import SparkStartupError, get_spark

log = logging.getLogger("run_all")
INTEGER_COLUMNS = {"vehicle_count", "parking_capacity", "occupied_spaces"}


def to_pandas(df: DataFrame, limit: int):
    """Collect a small aggregated result. Timestamps/dates become ISO strings (UTC) to avoid tz surprises."""
    for f in df.schema.fields:
        if isinstance(f.dataType, TimestampType):
            df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        elif isinstance(f.dataType, DateType):
            df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd"))
    return df.limit(limit).toPandas()


def read_local_csv(spark, path: Path) -> tuple[DataFrame, DataFrame]:
    """Validate a CSV and return normalized readings plus a sensor registry for offline analytics."""
    raw = load_csv(path)
    result = validate_dataframe(raw)
    write_rejects(result.rejected, REJECTS_CSV)
    if result.valid.empty:
        raise ValueError(f"No valid sensor readings found in {path}")

    fields = []
    for column in CSV_COLUMNS:
        if column in {"sensor_id", "location_id", "sensor_type"}:
            data_type = StringType()
        elif column == "timestamp":
            data_type = TimestampType()
        elif column in INTEGER_COLUMNS:
            data_type = IntegerType()
        else:
            data_type = DoubleType()
        fields.append(StructField(column, data_type, nullable=True))
    schema = StructType(fields)
    rows = []
    for values in result.valid[CSV_COLUMNS].itertuples(index=False, name=None):
        converted = []
        for column, value in zip(CSV_COLUMNS, values):
            if pd.isna(value):
                converted.append(None)
            elif column == "timestamp":
                converted.append(pd.Timestamp(value).to_pydatetime().astimezone(timezone.utc).replace(tzinfo=None))
            elif column in INTEGER_COLUMNS:
                converted.append(int(value))
            elif column in METRIC_COLUMNS or column in {"latitude", "longitude"}:
                converted.append(float(value))
            else:
                converted.append(str(value))
        rows.append(tuple(converted))

    readings = (spark.createDataFrame(rows, schema).withColumnRenamed("timestamp", "ts")
                .withColumn("day", F.to_date("ts")))
    readings = prepare_readings(readings)
    latest = Window.partitionBy("sensor_id").orderBy(F.desc("ts"))
    status = (readings.withColumn("rn", F.row_number().over(latest)).filter(F.col("rn") == 1)
              .select("sensor_id", "location_id", "sensor_type", F.col("ts").alias("last_seen"),
                      "latitude", "longitude",
                      F.when(F.col("sensor_type") == "traffic", F.col("vehicle_count"))
                      .when(F.col("sensor_type") == "environment", F.col("temperature"))
                      .when(F.col("sensor_type") == "parking", F.col("parking_occupancy"))
                      .otherwise(F.col("waste_fill_percentage")).alias("latest_value")))
    return readings, status


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--days", type=int, default=7, help="look-back window ending at the newest data day")
    ap.add_argument("--baseline", choices=["sensor", "daypart"], default="daypart")
    ap.add_argument("--csv", type=Path, help="run offline analytics from a validated CSV instead of Cassandra")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    s = get_settings()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    t_all = time.perf_counter()
    manifest = {"jobs": {}, "ok": True}
    spark = None
    try:
        offline = a.csv is not None
        spark = get_spark("SmartCityAnalytics", use_cassandra=not offline)
        if offline:
            all_readings, status = read_local_csv(spark, a.csv)
            status = status.cache()
        else:
            status = read_status(spark).cache()
            all_readings = None
        if status.limit(1).count() == 0:
            log.error("No sensors found. Check the input CSV or ingest data into Cassandra first.")
            return 1
        newest = status.agg(F.date_format(F.max("last_seen"), "yyyy-MM-dd").alias("d")).first()["d"]
        end = date.fromisoformat(a.end) if a.end else date.fromisoformat(newest)
        start = date.fromisoformat(a.start) if a.start else end - timedelta(days=a.days - 1)
        days = date_range(start, end)
        locations = [r["location_id"] for r in status.select("location_id").distinct().collect()]
        log.info("Window %s..%s (%d days), %d locations", start, end, len(days), len(locations))

        source = (all_readings.filter(F.col("day").isin(days)) if offline
                  else prepare_readings(read_readings(spark, locations, days)))
        readings = source.persist(StorageLevel.MEMORY_AND_DISK)
        t0 = time.perf_counter()
        n = readings.count()
        manifest.update(rows_after_cleaning=n, read_seconds=round(time.perf_counter() - t0, 2),
                        duplicate_keys=duplicate_report(readings))
        if n == 0:
            log.error("No readings in the selected window. Check the date range or ingest data.")
            return 1

        anomalies, an_summary = anomaly_detection.detect(readings, s.z_threshold, s.min_observations, a.baseline)
        health_tables = sensor_health_analysis.analyze(spark, status, readings, str(REJECTS_CSV),
                                                       s.active_minutes, s.inactive_minutes, s.health_reference)
        jobs = {
            "traffic": lambda: traffic_analysis.analyze(readings, s.traffic_surge_pct),
            "environment": lambda: environmental_analysis.analyze(readings, s.max_result_rows),
            "parking": lambda: parking_analysis.analyze(readings, s.parking_high_demand),
            "waste": lambda: waste_analysis.analyze(readings, s.waste_warning, s.waste_critical),
            "sensor_health": lambda: health_tables,
            "anomalies": lambda: {"anomaly_summary": an_summary, "data_quality_missing": missing_value_report(readings)},
        }
        for name, fn in jobs.items():
            t = time.perf_counter()
            try:
                tables = {}
                for tname, df in fn().items():
                    pdf = to_pandas(df, s.max_result_rows)
                    pdf.to_csv(RESULTS_DIR / f"{tname}.csv", index=False)
                    tables[tname] = len(pdf)
                if name == "sensor_health" and not offline:
                    sensor_health_analysis.persist_status(health_tables["sensor_health"])
                if name == "anomalies":
                    if offline:
                        anomaly_pdf = to_pandas(anomalies, s.max_result_rows)
                        anomaly_pdf["status"] = "new"
                        anomaly_pdf.to_csv(RESULTS_DIR / "detected_anomalies.csv", index=False)
                        tables["detected_anomalies"] = len(anomaly_pdf)
                    else:
                        anomaly_detection.persist(anomalies)
                        tables["anomalies_written_to_cassandra"] = anomalies.count()
                manifest["jobs"][name] = {"status": "ok", "seconds": round(time.perf_counter() - t, 2), "rows": tables}
                log.info("Job %-13s done in %.2fs %s", name, time.perf_counter() - t, tables)
            except Exception as exc:  # keep going; record the failure
                manifest["ok"] = False
                manifest["jobs"][name] = {"status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
                log.exception("Job %s failed", name)
        manifest.update(source="local_csv" if offline else "cassandra",
                        generated_at=datetime.now(timezone.utc).isoformat(), start=str(start), end=str(end),
                        locations=len(locations), total_seconds=round(time.perf_counter() - t_all, 2),
                        health_reference=s.health_reference, z_threshold=s.z_threshold, baseline=a.baseline,
                        thresholds={"waste_warning": s.waste_warning, "waste_critical": s.waste_critical,
                                    "parking_high_demand": s.parking_high_demand,
                                    "active_minutes": s.active_minutes, "inactive_minutes": s.inactive_minutes})
        (RESULTS_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))
        log.info("All done in %.2fs (ok=%s)", manifest["total_seconds"], manifest["ok"])
        return 0 if manifest["ok"] else 1
    except SparkStartupError as exc:
        log.error("%s", exc)
        return 2
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)  # full detail stays in the log, not in the dashboard
        return 1
    finally:
        if spark is not None:
            spark.stop()
        LOCK_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
