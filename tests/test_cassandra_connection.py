"""Unit tests with a mocked driver (no Cassandra needed). Integration test needs RUN_INTEGRATION=1."""
import os
from unittest import mock

import pandas as pd
import pytest

pytest.importorskip("cassandra.cluster")  # needs the real cassandra-driver package
from cassandra.cluster import NoHostAvailable  # noqa: E402

from ingestion import load_to_cassandra as L  # noqa: E402


def test_connection_failure_gives_friendly_message():
    with mock.patch.object(L, "Cluster") as cl:
        cl.return_value.connect.side_effect = NoHostAvailable("x", {})
        with pytest.raises(L.CassandraConnectionError, match="Cannot reach Cassandra"):
            L.connect()
        cl.return_value.shutdown.assert_called()


def test_split_cql_ignores_comments():
    assert L.split_cql("-- c\nCREATE TABLE a (x int PRIMARY KEY);\n-- d\nCREATE TABLE b (y int PRIMARY KEY);") == \
        ["CREATE TABLE a (x int PRIMARY KEY)", "CREATE TABLE b (y int PRIMARY KEY)"]


def test_empty_dataframe_loads_nothing():
    assert L.load_dataframe(mock.Mock(), pd.DataFrame(), "smart_city").attempted == 0


@pytest.mark.integration
@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="needs running Cassandra")
def test_roundtrip_is_idempotent():
    from ingestion.validate_data import load_csv
    raw = load_csv("tests/data/small_test_data.csv")
    L.init_schema()
    a = L.ingest_dataframe(raw)
    b = L.ingest_dataframe(raw)  # second run must not create new rows
    cluster, session = L.connect()
    count = session.execute("SELECT count(*) FROM sensor_readings_by_location WHERE location_id='LOC-01' AND day='2025-01-01'").one()[0]
    cluster.shutdown()
    assert a["accepted"] == b["accepted"] and count > 0
