"""Load validated readings into Apache Cassandra (idempotent upserts).

  python -m ingestion.load_to_cassandra --init-schema
  python -m ingestion.load_to_cassandra --csv data/sample_sensor_data.csv
  python -m ingestion.load_to_cassandra --generate --records 5000 --seed 7
"""
from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass

import pandas as pd
from cassandra import InvalidRequest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args

from config import (PRIMARY_METRIC, REJECTS_CSV, SAMPLE_CSV, SCHEMA_FILE, UNITS, Settings,
                    get_settings)
from ingestion.validate_data import ValidationError, load_csv, validate_dataframe, write_rejects

log = logging.getLogger("loader")


class CassandraConnectionError(RuntimeError):
    """Raised with a human-readable explanation when Cassandra cannot be used."""


def connect(settings: Settings | None = None, use_keyspace: bool = True):
    """Return (cluster, session). Caller must call cluster.shutdown()."""
    s = settings or get_settings()
    auth = PlainTextAuthProvider(s.username, s.password) if s.username else None
    cluster = None
    try:
        cluster = Cluster([s.cassandra_host], port=s.cassandra_port, auth_provider=auth,
                          connect_timeout=10)
        session = cluster.connect(s.keyspace) if use_keyspace else cluster.connect()
        return cluster, session
    except NoHostAvailable as exc:
        _close(cluster)
        raise CassandraConnectionError(
            f"Cannot reach Cassandra at {s.cassandra_host}:{s.cassandra_port}. Is the container/service "
            "running (docker compose up -d) and are CASSANDRA_HOST/PORT correct?") from exc
    except InvalidRequest as exc:
        _close(cluster)
        raise CassandraConnectionError(
            f"Keyspace '{s.keyspace}' does not exist. Run: python -m ingestion.load_to_cassandra --init-schema") from exc
    except Exception as exc:
        _close(cluster)
        raise CassandraConnectionError(f"Cassandra connection failed ({type(exc).__name__}): {exc}") from exc


def _close(cluster):
    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:  # pragma: no cover
            pass


def split_cql(text: str) -> list[str]:
    no_comments = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def init_schema(settings: Settings | None = None) -> int:
    """Create keyspace and tables from cassandra/schema.cql. Returns number of statements run."""
    s = settings or get_settings()
    statements = split_cql(SCHEMA_FILE.read_text(encoding="utf-8"))
    cluster, session = connect(s, use_keyspace=False)
    try:
        for stmt in statements:
            session.execute(re.sub(r"\bsmart_city\b", s.keyspace, stmt))
        log.info("Schema initialised in keyspace '%s' (%d statements)", s.keyspace, len(statements))
        return len(statements)
    finally:
        cluster.shutdown()


def _v(x):
    """pandas NaN / NA -> None"""
    return None if pd.isna(x) else x


def _primary(row) -> float | None:
    v = _v(row[PRIMARY_METRIC[row["sensor_type"]]])
    return None if v is None else float(v)


def reading_params(row) -> tuple:
    ts = row["timestamp"].to_pydatetime()
    f = lambda c: None if _v(row[c]) is None else float(row[c])
    i = lambda c: None if _v(row[c]) is None else int(row[c])
    return (row["location_id"], ts.date(), ts, row["sensor_id"], row["sensor_type"],
            f("temperature"), f("humidity"), f("noise_level"), i("vehicle_count"), f("average_speed"),
            i("parking_capacity"), i("occupied_spaces"), f("parking_occupancy"), f("waste_fill_percentage"))


def type_params(row) -> tuple:
    ts = row["timestamp"].to_pydatetime()
    return (row["sensor_type"], ts.date(), ts, row["location_id"], row["sensor_id"], _primary(row),
            UNITS[row["sensor_type"]])


@dataclass
class LoadStats:
    attempted: int = 0
    written: int = 0
    failed: int = 0
    seconds: float = 0.0


def load_dataframe(session, df: pd.DataFrame, keyspace: str, batch_size: int = 500,
                   progress=None) -> LoadStats:
    """Write validated rows to the 3 reading tables + latest_sensor_status.

    * Plain prepared INSERTs are upserts: the primary key makes re-runs idempotent.
    * Writes are sent concurrently per partition (NOT as multi-partition BATCHes, which are an anti-pattern).
    * latest_sensor_status uses USING TIMESTAMP = reading time, so the newest reading wins
      regardless of load order (Cassandra is last-write-wins by write timestamp).
    """
    stats = LoadStats()
    if df.empty:
        log.warning("Nothing to load (empty dataset)")
        return stats
    t0 = time.perf_counter()
    ins_loc = session.prepare(
        f"INSERT INTO {keyspace}.sensor_readings_by_location (location_id, day, ts, sensor_id, sensor_type, "
        "temperature, humidity, noise_level, vehicle_count, average_speed, parking_capacity, occupied_spaces, "
        "parking_occupancy, waste_fill_percentage) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    ins_type = session.prepare(
        f"INSERT INTO {keyspace}.sensor_readings_by_type (sensor_type, day, ts, location_id, sensor_id, "
        "reading_value, unit) VALUES (?,?,?,?,?,?,?)")
    ins_status = session.prepare(
        f"INSERT INTO {keyspace}.latest_sensor_status (sensor_id, location_id, sensor_type, last_seen, "
        "latest_value, latitude, longitude) VALUES (?,?,?,?,?,?,?) USING TIMESTAMP ?")

    total = len(df)
    for lo in range(0, total, batch_size):
        chunk = df.iloc[lo: lo + batch_size]
        for stmt, fn in ((ins_loc, reading_params), (ins_type, type_params)):
            params = [fn(r) for _, r in chunk.iterrows()]
            for ok, res in execute_concurrent_with_args(session, stmt, params, concurrency=64,
                                                        raise_on_first_error=False):
                stats.attempted += 1
                if ok:
                    stats.written += 1
                else:
                    stats.failed += 1
                    log.error("Write failed: %s", res)
        done = min(lo + batch_size, total)
        log.info("Progress: %d/%d rows (%.0f%%)", done, total, 100 * done / total)
        if progress:
            progress(done, total)

    latest = df.loc[df.groupby("sensor_id")["timestamp"].idxmax()]
    params = []
    for _, r in latest.iterrows():
        ts = r["timestamp"].to_pydatetime()
        lat = None if _v(r["latitude"]) is None else float(r["latitude"])
        lon = None if _v(r["longitude"]) is None else float(r["longitude"])
        params.append((r["sensor_id"], r["location_id"], r["sensor_type"], ts, _primary(r), lat, lon,
                       int(ts.timestamp() * 1_000_000)))
    for ok, res in execute_concurrent_with_args(session, ins_status, params, raise_on_first_error=False):
        stats.attempted += 1
        stats.written += bool(ok)
        stats.failed += (not ok)
        if not ok:
            log.error("Status write failed: %s", res)
    stats.seconds = time.perf_counter() - t0
    log.info("Loaded %d writes (%d failed) in %.2fs", stats.written, stats.failed, stats.seconds)
    return stats


def ingest_dataframe(raw: pd.DataFrame, settings: Settings | None = None, batch_size: int = 500,
                     rejects_path=REJECTS_CSV, progress=None) -> dict:
    """validate -> write rejects -> load. Returns a summary dict (used by CLI and dashboard)."""
    s = settings or get_settings()
    result = validate_dataframe(raw)
    write_rejects(result.rejected, rejects_path)
    summary = dict(result.summary)
    if result.valid.empty:
        summary.update(written=0, failed=0, seconds=0.0)
        return summary
    cluster, session = connect(s)
    try:
        stats = load_dataframe(session, result.valid, s.keyspace, batch_size, progress)
    finally:
        cluster.shutdown()
    summary.update(written=stats.written, failed=stats.failed, seconds=round(stats.seconds, 2))
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Validate and load sensor readings into Cassandra")
    p.add_argument("--init-schema", action="store_true", help="create keyspace + tables")
    p.add_argument("--csv", default=str(SAMPLE_CSV))
    p.add_argument("--generate", action="store_true", help="generate synthetic data instead of reading --csv")
    p.add_argument("--records", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--dry-run", action="store_true", help="validate only, do not write to Cassandra")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if a.init_schema:
            init_schema()
            if not a.generate and a.csv == str(SAMPLE_CSV) and not a.dry_run:
                return 0  # schema only
        if a.generate:
            from ingestion.generate_sensor_data import GeneratorConfig, build_dataframe
            raw = build_dataframe(GeneratorConfig(records=a.records, seed=a.seed)).astype(str).replace("<NA>", "")
        else:
            raw = load_csv(a.csv)
        if a.dry_run:
            res = validate_dataframe(raw)
            write_rejects(res.rejected, REJECTS_CSV)
            print(res.summary)
            return 0
        print(ingest_dataframe(raw, batch_size=a.batch_size))
        return 0
    except (ValidationError, CassandraConnectionError) as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
