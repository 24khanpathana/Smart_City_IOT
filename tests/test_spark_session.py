from unittest import mock

import pytest

from spark_jobs import spark_session as S


def test_missing_java_gives_clear_error(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    with mock.patch.object(S.shutil, "which", return_value=None):
        with pytest.raises(S.SparkStartupError, match="Java"):
            S.get_spark()


def test_connector_and_cassandra_settings_are_applied(monkeypatch):
    pytest.importorskip("pyspark")
    monkeypatch.setenv("CASSANDRA_HOST", "10.1.2.3")
    with mock.patch.object(S, "check_prerequisites"), mock.patch("pyspark.sql.SparkSession.builder") as b:
        b.appName.return_value = b.master.return_value = b.config.return_value = b
        S.get_spark()
        conf = {c.args[0]: c.args[1] for c in b.config.call_args_list}
    assert conf["spark.cassandra.connection.host"] == "10.1.2.3"
    assert "spark-cassandra-connector" in conf["spark.jars.packages"]
