"""Dashboard data access: small Spark result files + bounded Cassandra queries. No raw stack traces reach the UI."""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import LOCK_FILE, PIPELINE_LOG, RESULTS_DIR, ROOT, SAMPLE_CSV, get_settings

log = logging.getLogger("dashboard")
TIME_COLS = ("hour_start", "ts", "last_seen", "detected_at")


@st.cache_data(ttl=20, show_spinner=False)
def load_result(name: str) -> pd.DataFrame:
    path = RESULTS_DIR / f"{name}.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    for c in TIME_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    if "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"], errors="coerce").dt.date
    return df


@st.cache_data(ttl=20, show_spinner=False)
def load_manifest() -> dict | None:
    path = RESULTS_DIR / "manifest.json"
    return json.loads(path.read_text()) if path.exists() else None


@st.cache_resource(ttl=30, show_spinner=False)
def _cassandra():
    """(session, error_message). Cached briefly so a down database does not slow every rerun."""
    from ingestion.load_to_cassandra import CassandraConnectionError, connect
    try:
        _, session = connect(get_settings())
        return session, None
    except CassandraConnectionError as exc:
        return None, str(exc)
    except Exception as exc:  # config errors etc.
        log.exception("Cassandra connect failed")
        return None, f"Configuration problem: {exc}"


def db_status() -> tuple[bool, str]:
    session, err = _cassandra()
    return (True, f"Connected (keyspace '{get_settings().keyspace}')") if session else (False, err or "Not connected")


def fetch_anomalies(start: date, end: date) -> pd.DataFrame:
    """One partition read per day (bounded to 31 days) - no scans, no ALLOW FILTERING."""
    manifest = load_manifest()
    if manifest and manifest.get("source") == "local_csv":
        df = load_result("detected_anomalies")
        if df.empty:
            return df
        if "status" not in df:
            df["status"] = "new"
        else:
            df["status"] = df["status"].fillna("new")
        if "day" in df:
            df = df[(df["day"] >= start) & (df["day"] <= end)]
        return df
    session, err = _cassandra()
    if not session:
        raise ConnectionError(err)
    if (end - start).days > 30:
        start = end - timedelta(days=30)
    ks = get_settings().keyspace
    stmt = session.prepare(  # parameterised; the partition key (day) is always bound
        f"SELECT day, ts, anomaly_id, sensor_id, location_id, sensor_type, metric, observed_value, expected_range, "
        f"z_score, detection_method, severity, status FROM {ks}.detected_anomalies WHERE day = ?")
    rows = []
    for i in range((end - start).days + 1):
        rows.extend(session.execute(stmt, (start + timedelta(days=i),)))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["status"] = df["status"].fillna("new")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["day"] = df["day"].astype(str)
    return df


def update_anomaly_status(rows: pd.DataFrame, status: str) -> int:
    manifest = load_manifest()
    if manifest and manifest.get("source") == "local_csv":
        path = RESULTS_DIR / "detected_anomalies.csv"
        df = pd.read_csv(path)
        selected = set(rows["anomaly_id"])
        df.loc[df["anomaly_id"].isin(selected), "status"] = status
        df.to_csv(path, index=False)
        load_result.clear()
        return len(selected)
    session, err = _cassandra()
    if not session:
        raise ConnectionError(err)
    ks = get_settings().keyspace
    stmt = session.prepare(f"UPDATE {ks}.detected_anomalies SET status = ? WHERE day = ? AND ts = ? AND anomaly_id = ?")
    for _, r in rows.iterrows():
        session.execute(stmt, (status, date.fromisoformat(str(r["day"])), r["ts"].to_pydatetime(), r["anomaly_id"]))
    return len(rows)


# ---- pipeline control: one job at a time, started only by a button click ----
def pipeline_running() -> bool:
    if not LOCK_FILE.exists():
        return False
    if time.time() - LOCK_FILE.stat().st_mtime > 2 * 3600:  # stale lock
        LOCK_FILE.unlink(missing_ok=True)
        return False
    return True


def start_pipeline(days: int = 7, local_csv: bool = False) -> bool:
    if pipeline_running():
        return False
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCK_FILE.write_text(str(os.getpid()))
    log_file = open(PIPELINE_LOG, "w")
    command = [sys.executable, "-m", "spark_jobs.run_all", "--days", str(days)]
    if local_csv:
        command.extend(["--csv", str(SAMPLE_CSV)])
    subprocess.Popen(command, cwd=ROOT,
                     stdout=log_file, stderr=subprocess.STDOUT)
    return True


def pipeline_log_tail(lines: int = 15) -> str:
    return "\n".join(PIPELINE_LOG.read_text(errors="ignore").splitlines()[-lines:]) if PIPELINE_LOG.exists() else ""
