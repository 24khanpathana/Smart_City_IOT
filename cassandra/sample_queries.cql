-- All queries below use the full partition key (no ALLOW FILTERING, no full scans).
-- Run in cqlsh:  docker exec -it smartcity-cassandra cqlsh
USE smart_city;

-- 1. Readings of one location on one day (latest first)
SELECT ts, sensor_id, sensor_type, vehicle_count, average_speed, temperature
FROM sensor_readings_by_location
WHERE location_id = 'LOC-01' AND day = '2025-01-02' LIMIT 50;

-- 2. Same location, one afternoon (clustering-column range)
SELECT ts, sensor_id, vehicle_count FROM sensor_readings_by_location
WHERE location_id = 'LOC-01' AND day = '2025-01-02'
  AND ts >= '2025-01-02 12:00:00+0000' AND ts < '2025-01-02 18:00:00+0000';

-- 3. Latest state of one sensor
SELECT * FROM latest_sensor_status WHERE sensor_id = 'TRF-001';

-- 4. Traffic measurements on a day
SELECT ts, location_id, sensor_id, reading_value, unit
FROM sensor_readings_by_type WHERE sensor_type = 'traffic' AND day = '2025-01-02' LIMIT 100;

-- 5. Potential anomalies of a day
SELECT ts, sensor_id, metric, observed_value, expected_range, severity, status
FROM detected_anomalies WHERE day = '2025-01-03' LIMIT 100;

-- 6. Mark an anomaly as reviewed (needs the full primary key)
-- UPDATE detected_anomalies SET status = 'reviewed'
--   WHERE day = '2025-01-03' AND ts = '2025-01-03 08:15:00+0000' AND anomaly_id = '<id>';
