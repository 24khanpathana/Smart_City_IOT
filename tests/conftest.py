import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Local Spark WITHOUT Cassandra - used to test the analytics functions on in-memory DataFrames."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()
