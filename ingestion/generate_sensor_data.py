"""Synthetic IoT sensor data generator.

ALL VALUES ARE SYNTHETIC - they are not real-world measurements.
Run:  python -m ingestion.generate_sensor_data --records 10000 --seed 42
"""
from __future__ import annotations

import argparse
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from config import CSV_COLUMNS, SAMPLE_CSV, SENSOR_TYPES

log = logging.getLogger("generator")
PREFIX = {"traffic": "TRF", "environment": "ENV", "parking": "PRK", "waste": "WST"}


@dataclass
class GeneratorConfig:
    sensors: int = 20
    locations: int = 5
    records: int = 12_000
    start: str = "2025-01-01"
    end: str = "2025-01-15"
    interval_minutes: int = 15
    sensor_types: tuple = field(default_factory=lambda: SENSOR_TYPES)
    seed: int = 42
    anomaly_rate: float = 0.005    # extreme-but-valid readings
    missing_rate: float = 0.01     # random missing values
    duplicate_rate: float = 0.005  # exact duplicate rows
    invalid_rate: float = 0.003    # impossible values (negative counts, humidity > 100 ...)
    dropout_fraction: float = 0.1  # share of sensors that stop reporting early
    stream_offset: int = 0         # changes values (not layout) - used by periodic ingestion
    center_lat: float = 40.7128
    center_lon: float = -74.0060

    def validate(self) -> None:
        bad = [t for t in self.sensor_types if t not in SENSOR_TYPES]
        if bad:
            raise ValueError(f"Unknown sensor types: {bad}. Choose from {SENSOR_TYPES}")
        if not self.sensor_types:
            raise ValueError("At least one sensor type is required")
        if self.sensors < len(self.sensor_types):
            raise ValueError("sensors must be >= number of sensor types")
        if self.locations < 1 or self.records < 1 or self.interval_minutes < 1:
            raise ValueError("locations, records and interval_minutes must be >= 1")
        if pd.Timestamp(self.end) <= pd.Timestamp(self.start):
            raise ValueError("end must be after start")


def _rush(h: np.ndarray) -> np.ndarray:
    """Two commuter peaks (~08:30 and ~17:30)."""
    return np.exp(-((h - 8.5) ** 2) / (2 * 1.5 ** 2)) + 0.9 * np.exp(-((h - 17.5) ** 2) / (2 * 1.8 ** 2))


def build_dataframe(cfg: GeneratorConfig) -> pd.DataFrame:
    cfg.validate()
    layout = np.random.default_rng(cfg.seed)                          # sensor/location layout
    rng = np.random.default_rng(cfg.seed + 1000 + cfg.stream_offset)  # reading values

    start = pd.Timestamp(cfg.start, tz="UTC")
    end = pd.Timestamp(cfg.end, tz="UTC")
    step = pd.Timedelta(minutes=cfg.interval_minutes)
    steps_needed = math.ceil(cfg.records / cfg.sensors)
    steps_avail = int((end - start) / step)
    n = min(steps_needed, steps_avail)
    if n < steps_needed:
        log.warning("Date range only fits %d steps/sensor (needed %d): fewer records will be produced.",
                    n, steps_needed)
    times = start + pd.to_timedelta(np.arange(n) * cfg.interval_minutes, unit="m")
    hours = np.asarray(times.hour + times.minute / 60, dtype=float)
    weekend = np.asarray(times.dayofweek >= 5)
    rush = _rush(hours)

    locs = []
    for i in range(cfg.locations):
        locs.append({"id": f"LOC-{i + 1:02d}",
                     "lat": cfg.center_lat + layout.normal(0, 0.03),
                     "lon": cfg.center_lon + layout.normal(0, 0.03),
                     "traffic": layout.uniform(0.6, 1.5), "temp": layout.normal(0, 1.5),
                     "noise": layout.normal(0, 4)})

    n_drop = int(round(cfg.sensors * cfg.dropout_fraction))
    dropouts = set(layout.choice(cfg.sensors, size=n_drop, replace=False).tolist()) if n_drop else set()

    frames = []
    counters = {t: 0 for t in cfg.sensor_types}
    for i in range(cfg.sensors):
        stype = cfg.sensor_types[i % len(cfg.sensor_types)]
        counters[stype] += 1
        loc = locs[(i // len(cfg.sensor_types)) % cfg.locations]
        df = pd.DataFrame({"sensor_id": f"{PREFIX[stype]}-{counters[stype]:03d}", "location_id": loc["id"],
                           "sensor_type": stype, "timestamp": times,
                           "latitude": round(loc["lat"] + layout.normal(0, 0.002), 6),
                           "longitude": round(loc["lon"] + layout.normal(0, 0.002), 6)})
        for c in CSV_COLUMNS[6:]:
            df[c] = np.nan
        if stype == "traffic":
            base = layout.uniform(30, 80) * loc["traffic"]
            vol = base * (0.25 + 1.1 * rush) * np.where(weekend, 0.7, 1.0)
            count = rng.poisson(np.clip(vol, 1, None))
            load = np.clip(count / (base * 1.5), 0, 1.2)
            df["vehicle_count"] = count
            df["average_speed"] = np.clip(55 - 30 * load + rng.normal(0, 3, n), 5, 120).round(1)
        elif stype == "environment":
            temp = 22 + loc["temp"] + 7 * np.sin(2 * np.pi * (hours - 9) / 24) + rng.normal(0, 0.6, n)
            df["temperature"] = temp.round(2)
            df["humidity"] = np.clip(70 - 1.8 * (temp - 22) + rng.normal(0, 3, n), 20, 100).round(1)
            df["noise_level"] = np.clip(42 + loc["noise"] + 14 * rush + rng.normal(0, 2, n), 25, 120).round(1)
        elif stype == "parking":
            cap = int(layout.integers(60, 300))
            demand = np.exp(-((hours - 13) ** 2) / (2 * 4.5 ** 2)) * np.where(weekend, 0.6, 1.0)
            occ = np.round(cap * np.clip(0.15 + 0.7 * demand + rng.normal(0, 0.04, n), 0, 1)).astype(int)
            df["parking_capacity"] = cap
            df["occupied_spaces"] = occ
            df["parking_occupancy"] = (occ / cap * 100).round(2)
        else:  # waste: fills linearly, emptied when it wraps around 100 %
            rate = layout.uniform(0.8, 3.0) * cfg.interval_minutes / 60  # % per step
            fill = (layout.uniform(0, 90) + np.arange(n) * rate) % 100 + rng.normal(0, 0.5, n)
            df["waste_fill_percentage"] = np.clip(fill, 0, 100).round(1)
        if i in dropouts:  # sensor goes silent part-way (tests the health module)
            df = df.iloc[: max(1, int(n * layout.uniform(0.3, 0.8)))]
        frames.append(df)

    data = pd.concat(frames, ignore_index=True).sort_values(["timestamp", "sensor_id"], kind="stable")
    data = data.head(cfg.records).reset_index(drop=True)

    # ---- inject extreme-but-valid readings (potential anomalies) ----
    hit = rng.random(len(data)) < cfg.anomaly_rate
    t = data["sensor_type"]
    m = hit & (t == "traffic"); data.loc[m, "vehicle_count"] = data.loc[m, "vehicle_count"] * 4 + 50
    data.loc[m, "average_speed"] = (data.loc[m, "average_speed"] * 0.3).round(1)
    m = hit & (t == "environment"); data.loc[m, "temperature"] += 18; data.loc[m, "noise_level"] = np.clip(data.loc[m, "noise_level"] + 35, 0, 150)
    m = hit & (t == "parking"); data.loc[m, "occupied_spaces"] = data.loc[m, "parking_capacity"]; data.loc[m, "parking_occupancy"] = 100.0
    m = hit & (t == "waste"); data.loc[m, "waste_fill_percentage"] = np.clip(data.loc[m, "waste_fill_percentage"] + 45, 0, 100)

    # ---- missing values ----
    optional = ["vehicle_count", "average_speed", "temperature", "humidity", "noise_level",
                "occupied_spaces", "waste_fill_percentage"]
    for idx in np.flatnonzero(rng.random(len(data)) < cfg.missing_rate):
        cols = [c for c in optional if pd.notna(data.at[idx, c])]
        if cols:
            data.at[idx, cols[int(rng.integers(len(cols)))]] = np.nan

    # ---- invalid values ----
    for idx in np.flatnonzero(rng.random(len(data)) < cfg.invalid_rate):
        stype = data.at[idx, "sensor_type"]
        if stype == "traffic": data.at[idx, "vehicle_count"] = -5
        elif stype == "environment": data.at[idx, "humidity"] = 140
        elif stype == "parking": data.at[idx, "occupied_spaces"] = data.at[idx, "parking_capacity"] + 25
        else: data.at[idx, "waste_fill_percentage"] = 130

    # ---- duplicates ----
    dups = data[rng.random(len(data)) < cfg.duplicate_rate]
    data = pd.concat([data, dups], ignore_index=True)

    for c in ("vehicle_count", "parking_capacity", "occupied_spaces"):
        data[c] = data[c].round().astype("Int64")
    data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return data[CSV_COLUMNS]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Generate synthetic smart-city sensor data (CSV)")
    d = GeneratorConfig()
    p.add_argument("--sensors", type=int, default=d.sensors)
    p.add_argument("--locations", type=int, default=d.locations)
    p.add_argument("--records", type=int, default=d.records)
    p.add_argument("--start", default=d.start)
    p.add_argument("--end", default=d.end)
    p.add_argument("--interval-minutes", type=int, default=d.interval_minutes)
    p.add_argument("--types", nargs="+", default=list(d.sensor_types), choices=SENSOR_TYPES)
    p.add_argument("--seed", type=int, default=d.seed)
    p.add_argument("--output", type=Path, default=SAMPLE_CSV)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = GeneratorConfig(a.sensors, a.locations, a.records, a.start, a.end, a.interval_minutes,
                          tuple(a.types), a.seed)
    df = build_dataframe(cfg)
    a.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.output, index=False)
    log.info("Wrote %d synthetic rows to %s (seed=%d)", len(df), a.output, cfg.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
