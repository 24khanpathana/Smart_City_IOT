# Smart City IoT Sensor Data Analytics — Apache Cassandra + Apache Spark

## 1. Project overview
End-to-end pipeline: **synthetic IoT sensors / CSV → validation → Apache Cassandra → Spark Cassandra Connector → PySpark analytics → Streamlit dashboard.**
All sample data is **synthetic** (not real-world measurements).

## 2. Problem statement
Cities produce large volumes of sensor data (traffic, environment, parking, waste). This project shows how to store it in a distributed NoSQL database, process it with Spark, flag *potential* anomalies, and present results interactively.

## 3. Objectives
Distributed storage (Cassandra) · scalable processing (PySpark) · time-series analysis · statistical anomaly detection · interactive visualisation.

## 4. Features
* Configurable synthetic generator (sensors, locations, records, date range, interval, types, seed) with rush-hour patterns, missing values, duplicates, invalid values, unusual readings and sensors that go silent.
* Validation with reject log; **idempotent** loading (primary-key upserts — re-running never duplicates).
* Query-oriented Cassandra tables (no `ALLOW FILTERING`, no dashboard full scans, prepared statements).
* PySpark jobs: traffic (volume vs. congestion), environment, parking, waste, sensor health, Z-score anomalies.
* Dashboard: 8 pages, date/location filters, anomaly review workflow (new → reviewed/dismissed stored in Cassandra).
* **Mode A (batch, mandatory)** and **Mode B (periodic simulated near-real-time)**.

## 5. Technology stack (pinned, compatible set)
| Component | Version |
|---|---|
| Python | 3.10 or 3.11 (3.12+: cassandra-driver is unreliable) |
| Java | JDK 17 |
| Apache Spark / PySpark | 3.5.1 (`pip install pyspark`) |
| Spark Cassandra Connector | `spark-cassandra-connector_2.12:3.5.1` (downloaded by Spark on first run) |
| Apache Cassandra | 4.1 (Docker image `cassandra:4.1`) |
| cassandra-driver | 3.29.1 |
| Streamlit / Plotly / pandas | ≥1.36 / ≥5.24 / 2.x |

## 6. Architecture
```
Generator / CSV → ingestion/validate_data.py → ingestion/load_to_cassandra.py → Cassandra (smart_city)
                                                                                    │ Spark Cassandra Connector
                                         spark_jobs/*.py (PySpark) ◄────────────────┘
                                           │  small aggregated results → data/processed/results/*.csv + manifest.json
                                           │  anomalies + sensor status → back into Cassandra
                                           ▼
                                   dashboard/ (Streamlit)  ← reads result files + bounded Cassandra queries
```
Ingestion, database, Spark and dashboard are separate modules. The dashboard never starts ingestion/Spark on a page rerun — only on an explicit button click, guarded by a lock file.

## 7. Folder structure
```
SmartCity-IoT-Analytics/
├── config.py                    # all settings from environment variables
├── data/ (raw/, processed/, sample_sensor_data.csv)
├── cassandra/ (schema.cql, sample_queries.cql)   # do NOT add __init__.py here (would shadow the driver)
├── ingestion/ (generate_sensor_data.py, validate_data.py, load_to_cassandra.py, periodic_ingest.py)
├── spark_jobs/ (spark_session, read_from_cassandra, traffic/environmental/parking/waste/sensor_health/anomaly modules, run_all.py)
├── dashboard/ (app.py, pages/, components/)
├── tests/ (+ tests/data/small_test_data.csv)
├── docker-compose.yml, .env.example, .gitignore, requirements.txt, pytest.ini
```
Column note: the reading timestamp is stored as `ts` in Cassandra (the CSV column is `timestamp`).

## 8. Prerequisites (Windows 10/11)
1. **Docker Desktop** (runs Cassandra; Cassandra is not natively supported on Windows).
2. **JDK 17** (e.g. Eclipse Temurin) and `JAVA_HOME` set.
3. **Python 3.10/3.11**, **Git**, VS Code.
4. Internet access once (Spark downloads the connector from Maven; pip downloads packages).

## 9–14. Installation
```powershell
git clone <your-repo-url> SmartCity-IoT-Analytics ; cd SmartCity-IoT-Analytics
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env          # edit only if your Cassandra differs
docker compose up -d            # Cassandra; wait ~60 s until healthy
docker logs smartcity-cassandra --tail 5
```
* **Spark**: pip's `pyspark` bundles Spark 3.5.1 (local mode); no separate install needed. On Windows, if Spark prints winutils/HADOOP_HOME warnings, download matching `winutils.exe` + `hadoop.dll` for Hadoop 3.3 into `C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`.
* **Connector config** is automatic (`spark_jobs/spark_session.py`): `spark.jars.packages`, `spark.cassandra.connection.host/port`, optional auth, SQL extensions, UTC session time zone.
* **Env vars**: see `.env.example` (placeholders only; `.env` is git-ignored).

## 15. Initialise the schema
```powershell
python -m ingestion.load_to_cassandra --init-schema
```
Multi-node deployment: change replication to `NetworkTopologyStrategy` (e.g. `{'dc1': 3}`), use `LOCAL_QUORUM`, add nodes, run `nodetool repair`. A single local node gives **no** fault tolerance.

## 16. Generate data
```powershell
python -m ingestion.generate_sensor_data --records 12000 --sensors 20 --locations 5 --seed 42
```
`data/sample_sensor_data.csv` (11,343 rows incl. injected duplicates/invalid rows) is already included.

## 17. Ingest (Mode A, step 1)
```powershell
python -m ingestion.load_to_cassandra --csv data/sample_sensor_data.csv
python -m ingestion.load_to_cassandra --dry-run          # validate only
python -m ingestion.load_to_cassandra --generate --records 5000 --seed 7   # synthetic ingestion
```
Rejected rows + reasons: `data/processed/rejected_records.csv`.

## 18. Run PySpark jobs (Mode A, step 2)
```powershell
python -m spark_jobs.run_all                         # newest 7 days in Cassandra
python -m spark_jobs.run_all --start 2025-01-02 --end 2025-01-04 --baseline daypart
python -m spark_jobs.run_all --csv data/sample_sensor_data.csv  # offline demo; no Cassandra required
```
The first Cassandra-backed run downloads the connector; offline CSV mode does not need it. Per-job timings are
logged and stored in `manifest.json`.
When Cassandra is unavailable, Data Management offers **Run local sample analytics**; this generates dashboard
results from the included CSV without connecting to Cassandra. Offline anomaly reviews are saved to the local
results CSV; Cassandra-backed ingestion and persistence require Cassandra to be started.

## 19. Dashboard
```powershell
streamlit run dashboard/app.py
```
**Mode B (periodic, simulated live):** in a second terminal  
`python -m ingestion.periodic_ingest --cycles 10 --interval-seconds 30 --analytics-every 5`  
then refresh the dashboard. This simulates a stream; real MQTT/Kafka ingestion is **not** implemented (a producer would call `ingest_dataframe()`).

## 20. Tests
```powershell
pytest -m "not integration"          # unit tests (Spark tests need Java; driver tests need cassandra-driver)
$env:RUN_INTEGRATION="1"; pytest -m integration   # needs running Cassandra
```
| Test file | Needs |
|---|---|
| test_validate, test_generator, test_config | pandas only |
| test_spark_session, test_spark_analytics | pyspark + Java (no Cassandra) |
| test_cassandra_connection (mocked) | cassandra-driver; `integration` test needs Cassandra |

## 21. Common errors
| Symptom | Fix |
|---|---|
| `Cannot reach Cassandra at …` | `docker compose up -d`, wait for startup, check `.env` host/port |
| `Keyspace 'smart_city' does not exist` | run `--init-schema` |
| `Java was not found` / Spark won't start | install JDK 11/17, set `JAVA_HOME`, reopen terminal |
| Connector download fails | needs internet on first run; or pre-download the jar and set `SPARK_CONNECTOR_PACKAGE` accordingly |
| `ImportError` for `cassandra` on Python 3.12 | use Python 3.10/3.11 |
| Dashboard says "No analytics results" | run Data Management → *Run Spark analytics* |
| All sensors "inactive" with live data | set `HEALTH_REFERENCE=now`; for historic data keep `data` |

## 22. Expected output
After ingest: ~11,189 accepted / ~154 rejected rows (seed 42). After `run_all`: CSVs in `data/processed/results/`, anomalies in `detected_anomalies`, dashboard pages populated. Exact numbers vary with parameters.

## Status rules, thresholds, methods
* **Sensor status**: unknown (never seen) · active (≤30 min) · delayed (≤180 min) · inactive (>180 min), relative to the newest reading (`HEALTH_REFERENCE=data`) or the clock (`now`). Silent ≠ faulty.
* **Congestion** needs speed: *likely congested* = volume ≥ location's 75th percentile **and** speed < 60 % of its free-flow (90th-percentile) speed.
* **Anomalies**: `Z=(x−mean)/std` per sensor + metric + 6-hour day-part, threshold 3 (`ZSCORE_THRESHOLD`), ≥30 observations; zero std / short history skipped. Severity: medium 3–4, high 4–5, critical ≥5. These are *potential* anomalies, not incidents; environment outliers are statistical, **not** regulatory violations.
* **Waste thresholds** 80 % / 90 % are operational settings.

## 23. Limitations (honest list)
* **Not executed here:** the author's environment had no Cassandra, Spark or Streamlit available. Verified by execution: generator, validator, config logic (19 tests via a shim) and Python syntax of all files. **Cassandra loading, PySpark jobs, connector push-down, Streamlit pages and the Spark/driver tests have NOT been run** — expect to fix small issues on first run; use the checklist below.
* Partition push-down of `IN` filters should be confirmed with `.explain()`.
* Single-node Cassandra: no fault tolerance or scaling demonstrated; no Spark-vs-pandas or Cassandra-vs-other benchmark is claimed.
* Optional Isolation Forest, Kafka/MQTT, Structured Streaming and PM2.5/PM10 are **not implemented**. Waste time-to-full prediction intentionally omitted.
* `sensor_readings_by_type` partitions by (type, day): fine for the demo, add a bucket column for very large fleets.
* Re-running detection with a different threshold does not delete previously stored anomalies.

## 24. Future enhancements
MQTT/Kafka producer + Structured Streaming, Isolation Forest, PM2.5/PM10 sensors, time-bucketed partitions, multi-node Docker compose, authentication for the dashboard, fair benchmarks.

## Verification checklist
1. `docker ps` shows `smartcity-cassandra`; `docker exec -it smartcity-cassandra cqlsh -e "DESCRIBE KEYSPACE smart_city"` lists 4 tables.
2. After ingest: `cqlsh -e "SELECT count(*) FROM smart_city.sensor_readings_by_location WHERE location_id='LOC-01' AND day='2025-01-02'"` > 0; re-running ingest leaves the count unchanged.
3. `python -m spark_jobs.run_all` ends with `All done … ok=True`; `data/processed/results/manifest.json` exists.
4. `SELECT * FROM smart_city.detected_anomalies WHERE day='2025-01-03' LIMIT 5;` returns rows.
5. Dashboard: all 8 pages open; Data Management shows "Connected".
