-- Smart City IoT schema. Run via:  python -m ingestion.load_to_cassandra --init-schema
-- (The loader replaces "smart_city" with CASSANDRA_KEYSPACE.)
-- NOTE: do NOT add an __init__.py to this folder: it would shadow the 'cassandra' driver package.
--
-- Local dev: SimpleStrategy, RF=1 (single node, NO fault tolerance).
-- Multi-node production example:
--   CREATE KEYSPACE smart_city WITH replication =
--     {'class': 'NetworkTopologyStrategy', 'dc1': 3};
--   then use LOCAL_QUORUM for reads/writes and `nodetool repair` regularly.

CREATE KEYSPACE IF NOT EXISTS smart_city
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Table 1: readings by location. Query: "all readings of location X on day D, time-ordered".
-- Partition = (location_id, day) keeps partitions bounded (<= sensors_at_location * readings/day).
-- Column "ts" is the reading timestamp.  Primary key also gives idempotent upserts:
-- re-loading the same CSV overwrites identical rows instead of duplicating them.
CREATE TABLE IF NOT EXISTS smart_city.sensor_readings_by_location (
  location_id text,
  day date,
  ts timestamp,
  sensor_id text,
  sensor_type text,
  temperature double,
  humidity double,
  noise_level double,
  vehicle_count int,
  average_speed double,
  parking_capacity int,
  occupied_spaces int,
  parking_occupancy double,
  waste_fill_percentage double,
  PRIMARY KEY ((location_id, day), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts DESC, sensor_id ASC);

-- Table 2: latest known state per sensor. Query: "state of sensor S" / sensor registry.
-- "status" (active/delayed/inactive/unknown) is written by the Spark health job, not by ingestion.
CREATE TABLE IF NOT EXISTS smart_city.latest_sensor_status (
  sensor_id text PRIMARY KEY,
  location_id text,
  sensor_type text,
  last_seen timestamp,
  status text,
  latest_value double,
  latitude double,
  longitude double
);

-- Table 3: readings by type. Query: "primary measurements of type T on day D".
CREATE TABLE IF NOT EXISTS smart_city.sensor_readings_by_type (
  sensor_type text,
  day date,
  ts timestamp,
  location_id text,
  sensor_id text,
  reading_value double,
  unit text,
  PRIMARY KEY ((sensor_type, day), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts DESC, sensor_id ASC);

-- Table 4: potential anomalies. Query: "anomalies detected for day D" (dashboard iterates a bounded day range).
-- anomaly_id is a deterministic hash, so re-running detection upserts instead of duplicating.
-- "status" (new / reviewed / dismissed) is NOT written by Spark, so human review decisions survive re-runs.
CREATE TABLE IF NOT EXISTS smart_city.detected_anomalies (
  day date,
  ts timestamp,
  anomaly_id text,
  sensor_id text,
  location_id text,
  sensor_type text,
  metric text,
  observed_value double,
  expected_low double,
  expected_high double,
  expected_range text,
  z_score double,
  detection_method text,
  severity text,
  status text,
  detected_at timestamp,
  PRIMARY KEY ((day), ts, anomaly_id)
) WITH CLUSTERING ORDER BY (ts DESC, anomaly_id ASC);
