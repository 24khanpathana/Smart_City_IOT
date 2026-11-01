"""Central configuration. All values come from environment variables (.env supported)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
try:  # python-dotenv is optional at import time
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:  # pragma: no cover
    pass

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROCESSED_DIR / "results"
SAMPLE_CSV = DATA_DIR / "sample_sensor_data.csv"
REJECTS_CSV = PROCESSED_DIR / "rejected_records.csv"
LOCK_FILE = PROCESSED_DIR / ".pipeline.lock"
PIPELINE_LOG = PROCESSED_DIR / "pipeline.log"
SCHEMA_FILE = ROOT / "cassandra" / "schema.cql"

SENSOR_TYPES = ("traffic", "environment", "parking", "waste")

# Unified data model (one CSV row == one reading from one sensor)
REQUIRED_COLUMNS = ["sensor_id", "location_id", "sensor_type", "timestamp"]
METRIC_COLUMNS = [
    "vehicle_count", "average_speed", "temperature", "humidity", "noise_level",
    "parking_capacity", "occupied_spaces", "parking_occupancy", "waste_fill_percentage",
]
OPTIONAL_COLUMNS = ["latitude", "longitude"] + METRIC_COLUMNS
CSV_COLUMNS = ["sensor_id", "location_id", "sensor_type", "timestamp", "latitude", "longitude"] + METRIC_COLUMNS

# Physically plausible ranges. Values outside are INVALID (rejected / nulled), not "anomalies".
VALUE_RANGES = {
    "vehicle_count": (0, 5000), "average_speed": (0, 200), "temperature": (-50, 60),
    "humidity": (0, 100), "noise_level": (0, 150), "waste_fill_percentage": (0, 100),
}
PRIMARY_METRIC = {"traffic": "vehicle_count", "environment": "temperature",
                  "parking": "parking_occupancy", "waste": "waste_fill_percentage"}
UNITS = {"traffic": "vehicles", "environment": "celsius", "parking": "percent", "waste": "percent"}


class ConfigError(ValueError):
    """Raised when an environment variable is missing or invalid."""


def _num(name: str, default, cast):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    cassandra_host: str
    cassandra_port: int
    keyspace: str
    username: str | None
    password: str | None
    spark_master: str
    connector_package: str
    active_minutes: int
    inactive_minutes: int
    health_reference: str
    z_threshold: float
    min_observations: int
    waste_warning: float
    waste_critical: float
    parking_high_demand: float
    traffic_surge_pct: float
    max_result_rows: int

    def validate(self) -> "Settings":
        if not (1 <= self.cassandra_port <= 65535):
            raise ConfigError("CASSANDRA_PORT must be between 1 and 65535")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", self.keyspace):
            raise ConfigError("CASSANDRA_KEYSPACE must be lowercase letters/digits/underscore")
        if not self.cassandra_host:
            raise ConfigError("CASSANDRA_HOST must not be empty")
        if self.health_reference not in ("data", "now"):
            raise ConfigError("HEALTH_REFERENCE must be 'data' or 'now'")
        if not (0 < self.active_minutes < self.inactive_minutes):
            raise ConfigError("Require 0 < SENSOR_ACTIVE_MINUTES < SENSOR_INACTIVE_MINUTES")
        if self.z_threshold <= 0 or self.min_observations < 2:
            raise ConfigError("ZSCORE_THRESHOLD must be > 0 and MIN_OBSERVATIONS >= 2")
        if not (0 < self.waste_warning < self.waste_critical <= 100):
            raise ConfigError("Require 0 < WASTE_WARNING_PCT < WASTE_CRITICAL_PCT <= 100")
        return self


def get_settings() -> Settings:
    """Read settings from the environment on every call (keeps tests simple)."""
    return Settings(
        cassandra_host=os.getenv("CASSANDRA_HOST", "127.0.0.1").strip(),
        cassandra_port=_num("CASSANDRA_PORT", 9042, int),
        keyspace=os.getenv("CASSANDRA_KEYSPACE", "smart_city").strip(),
        username=os.getenv("CASSANDRA_USERNAME") or None,
        password=os.getenv("CASSANDRA_PASSWORD") or None,
        spark_master=os.getenv("SPARK_MASTER", "local[*]"),
        connector_package=os.getenv("SPARK_CONNECTOR_PACKAGE",
                                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"),
        active_minutes=_num("SENSOR_ACTIVE_MINUTES", 30, int),
        inactive_minutes=_num("SENSOR_INACTIVE_MINUTES", 180, int),
        health_reference=os.getenv("HEALTH_REFERENCE", "data").strip().lower(),
        z_threshold=_num("ZSCORE_THRESHOLD", 3.0, float),
        min_observations=_num("MIN_OBSERVATIONS", 30, int),
        waste_warning=_num("WASTE_WARNING_PCT", 80.0, float),
        waste_critical=_num("WASTE_CRITICAL_PCT", 90.0, float),
        parking_high_demand=_num("PARKING_HIGH_DEMAND_PCT", 80.0, float),
        traffic_surge_pct=_num("TRAFFIC_SURGE_PCT", 150.0, float),
        max_result_rows=_num("MAX_RESULT_ROWS", 5000, int),
    ).validate()
