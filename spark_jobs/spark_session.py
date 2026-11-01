"""SparkSession factory with the Spark Cassandra Connector configured from environment variables."""
from __future__ import annotations

import logging
import os
import shutil
import sys
from contextlib import contextmanager

from config import Settings, get_settings

log = logging.getLogger("spark")


class SparkStartupError(RuntimeError):
    """Spark could not be started; the message explains what to fix."""


def check_prerequisites() -> None:
    if not (shutil.which("java") or os.getenv("JAVA_HOME")):
        raise SparkStartupError("Java was not found. Install JDK 11 or 17 and set JAVA_HOME (see README).")
    if os.name == "nt" and not os.getenv("HADOOP_HOME"):
        log.warning("HADOOP_HOME is not set. On Windows Spark may need winutils.exe/hadoop.dll (see README).")


def get_spark(app_name: str = "SmartCityIoTAnalytics", settings: Settings | None = None,
              use_cassandra: bool = True):
    s = settings or get_settings()
    check_prerequisites()
    # Make workers use the same interpreter as this virtual environment
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SparkStartupError("pyspark is not installed. Run: pip install -r requirements.txt") from exc

    builder = (SparkSession.builder.appName(app_name).master(s.spark_master)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.ui.showConsoleProgress", "false"))
    if use_cassandra:
        builder = (builder
                   .config("spark.jars.packages", s.connector_package)
                   .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                   .config("spark.cassandra.connection.host", s.cassandra_host)
                   .config("spark.cassandra.connection.port", str(s.cassandra_port)))
    if use_cassandra and s.username:
        builder = (builder.config("spark.cassandra.auth.username", s.username)
                   .config("spark.cassandra.auth.password", s.password or ""))
    try:
        spark = builder.getOrCreate()
    except Exception as exc:
        raise SparkStartupError(
            f"Spark failed to start ({type(exc).__name__}). Common causes: wrong Java version (use 11/17), "
            f"no internet for the first connector download, or port conflicts. Detail: {exc}") from exc
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s started (master=%s)", spark.version, s.spark_master)
    return spark


@contextmanager
def spark_context(app_name: str = "SmartCityIoTAnalytics"):
    """with spark_context() as spark: ...  (always stops Spark)"""
    spark = get_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()
        log.info("Spark stopped")
