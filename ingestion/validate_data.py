"""Validation and cleaning of sensor readings (pure pandas, no database needed)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from config import CSV_COLUMNS, METRIC_COLUMNS, REQUIRED_COLUMNS, SENSOR_TYPES, VALUE_RANGES

log = logging.getLogger("validate")


class ValidationError(Exception):
    """The input as a whole cannot be processed (missing columns, unreadable file)."""


@dataclass
class ValidationResult:
    valid: pd.DataFrame      # cleaned rows, ready for Cassandra
    rejected: pd.DataFrame   # original rows + rejection_reason
    summary: dict


def load_csv(path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise ValidationError(f"CSV file not found: {path}")
    try:
        return pd.read_csv(path, dtype=str)  # read as text so malformed values can be reported
    except Exception as exc:
        raise ValidationError(f"Could not read CSV {path}: {exc}") from exc


def validate_dataframe(df: pd.DataFrame) -> ValidationResult:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing required column(s): {', '.join(missing)}. "
                              f"Required: {', '.join(REQUIRED_COLUMNS)}")
    if df.empty:
        log.warning("Dataset has no rows")
        empty = pd.DataFrame(columns=CSV_COLUMNS)
        return ValidationResult(empty, empty.assign(rejection_reason=[]),
                                {"total": 0, "accepted": 0, "rejected": 0, "duplicates": 0,
                                 "by_reason": {}, "by_type": {}})

    work = df.copy()
    reason = pd.Series("", index=work.index)

    def flag(mask, msg):
        mask = mask.fillna(False)
        reason.loc[mask] = reason.loc[mask] + msg + "; "

    for c in ("sensor_id", "location_id", "sensor_type"):
        work[c] = work[c].astype("string").str.strip()
    work["sensor_type"] = work["sensor_type"].str.lower()
    for c in ("sensor_id", "location_id"):
        flag(work[c].isna() | (work[c] == ""), f"missing {c}")
    flag(~work["sensor_type"].isin(SENSOR_TYPES), "unknown sensor_type")

    ts = pd.to_datetime(work["timestamp"], utc=True, errors="coerce", format="mixed")
    flag(ts.isna(), "invalid timestamp")
    work["timestamp"] = ts

    for c in ["latitude", "longitude"] + METRIC_COLUMNS:
        if c not in work.columns:
            work[c] = np.nan
            continue
        raw = work[c]
        num = pd.to_numeric(raw, errors="coerce")
        flag(raw.notna() & (raw.astype(str).str.strip() != "") & num.isna(), f"non-numeric {c}")
        work[c] = num
    flag(work["latitude"].notna() & ~work["latitude"].between(-90, 90), "latitude out of range")
    flag(work["longitude"].notna() & ~work["longitude"].between(-180, 180), "longitude out of range")

    # Range checks (impossible values are invalid, unusual-but-possible values are NOT rejected here)
    for col, (lo, hi) in VALUE_RANGES.items():
        flag(work[col].notna() & ((work[col] < lo) | (work[col] > hi)), f"{col} out of range [{lo}, {hi}]")

    # Missing-value policy by sensor type
    t = work["sensor_type"]
    flag((t == "traffic") & work["vehicle_count"].isna(), "traffic reading without vehicle_count")
    flag((t == "environment") & work[["temperature", "humidity", "noise_level"]].isna().all(axis=1),
         "environment reading without any measurement")
    cap, occ = work["parking_capacity"], work["occupied_spaces"]
    flag((t == "parking") & (cap.isna() | (cap <= 0)), "invalid parking capacity")
    flag((t == "parking") & occ.isna(), "parking reading without occupied_spaces")
    flag((t == "parking") & occ.notna() & cap.notna() & ((occ < 0) | (occ > cap)), "occupied_spaces outside 0..capacity")
    flag((t == "waste") & work["waste_fill_percentage"].isna(), "waste reading without fill percentage")

    # Duplicates (same sensor + timestamp) - only among rows that are otherwise valid
    ok = reason == ""
    dup = pd.Series(False, index=work.index)
    dup.loc[ok] = work.loc[ok].duplicated(["sensor_id", "timestamp"], keep="first")
    flag(dup, "duplicate (sensor_id, timestamp)")

    # Safe occupancy: never divides by zero / missing capacity
    safe = (t == "parking") & (cap > 0) & occ.notna()
    work["parking_occupancy"] = np.where(safe, (occ / cap.where(cap > 0)) * 100, np.nan).round(2)

    bad = reason != ""
    valid = work.loc[~bad, CSV_COLUMNS].reset_index(drop=True)
    rejected = df.loc[bad].copy()
    rejected["rejection_reason"] = reason[bad].str.rstrip("; ").values
    by_reason: dict = {}
    for r in rejected["rejection_reason"]:
        for part in r.split("; "):
            by_reason[part] = by_reason.get(part, 0) + 1
    summary = {"total": len(df), "accepted": len(valid), "rejected": int(bad.sum()),
               "duplicates": int(dup.sum()), "by_reason": by_reason,
               "by_type": valid["sensor_type"].value_counts().to_dict()}
    log.info("Validation: %d total, %d accepted, %d rejected (%d duplicates)",
             summary["total"], summary["accepted"], summary["rejected"], summary["duplicates"])
    return ValidationResult(valid, rejected, summary)


def write_rejects(rejected: pd.DataFrame, path) -> None:
    """Overwrite the reject log for this run (reasons are kept so users can fix the data)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rejected.to_csv(path, index=False)
    log.info("Wrote %d rejected records to %s", len(rejected), path)
