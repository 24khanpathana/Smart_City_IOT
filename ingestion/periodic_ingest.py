"""Mode B - periodic near-real-time ingestion (simulated sensors).

Every cycle: generate a few new synthetic readings -> validate -> write to Cassandra ->
(optionally) re-run the Spark analytics. A lock file prevents two instances running at once.
Start it from a terminal, NOT from Streamlit:
    python -m ingestion.periodic_ingest --cycles 10 --interval-seconds 30 --analytics-every 5
This is simulated live data; a real MQTT/Kafka producer would call ingest_dataframe() the same way.
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time

import pandas as pd

from config import PROCESSED_DIR, ROOT
from ingestion.generate_sensor_data import GeneratorConfig, build_dataframe
from ingestion.load_to_cassandra import CassandraConnectionError, ingest_dataframe

log = logging.getLogger("periodic")
FMT = "%Y-%m-%dT%H:%M:%S"
LOCK = PROCESSED_DIR / ".periodic.lock"


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--cycles", type=int, default=10)
    p.add_argument("--interval-seconds", type=float, default=30)
    p.add_argument("--steps-per-cycle", type=int, default=4, help="timestamps generated per cycle")
    p.add_argument("--analytics-every", type=int, default=0, help="re-run Spark every N cycles (0 = never)")
    p.add_argument("--start", default="2025-01-06T05:00:00", help="timestamp where the simulated stream begins")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK.exists():
        log.error("Another periodic ingest seems to be running (%s). Delete the file if it is stale.", LOCK)
        return 1
    LOCK.write_text(str(os.getpid()))
    try:
        cursor = pd.Timestamp(a.start, tz="UTC")
        base = GeneratorConfig()
        for cycle in range(a.cycles):
            end = cursor + pd.Timedelta(minutes=base.interval_minutes * a.steps_per_cycle)
            cfg = GeneratorConfig(records=base.sensors * a.steps_per_cycle, start=cursor.strftime(FMT), end=end.strftime(FMT),
                                  seed=a.seed, stream_offset=cycle + 1, dropout_fraction=0.0,
                                  duplicate_rate=0.0)
            raw = build_dataframe(cfg).astype(str).replace("<NA>", "")
            summary = ingest_dataframe(raw)
            log.info("Cycle %d/%d: %s", cycle + 1, a.cycles, summary)
            cursor = end
            if a.analytics_every and (cycle + 1) % a.analytics_every == 0:
                log.info("Re-running Spark analytics ...")
                subprocess.run([sys.executable, "-m", "spark_jobs.run_all", "--days", "3"], cwd=ROOT, check=False)
            if cycle + 1 < a.cycles:
                time.sleep(a.interval_seconds)
        return 0
    except CassandraConnectionError as exc:
        log.error("%s", exc)
        return 1
    finally:
        LOCK.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
