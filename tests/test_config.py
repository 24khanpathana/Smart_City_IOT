import pytest

from config import ConfigError, get_settings


def test_defaults(monkeypatch):
    for k in ("CASSANDRA_HOST", "CASSANDRA_PORT", "CASSANDRA_KEYSPACE"):
        monkeypatch.delenv(k, raising=False)
    s = get_settings()
    assert (s.cassandra_host, s.cassandra_port, s.keyspace) == ("127.0.0.1", 9042, "smart_city")


@pytest.mark.parametrize("name,value", [("CASSANDRA_PORT", "abc"), ("CASSANDRA_PORT", "99999"),
                                        ("CASSANDRA_KEYSPACE", "Bad-Name"), ("HEALTH_REFERENCE", "later"),
                                        ("SENSOR_ACTIVE_MINUTES", "500")])
def test_invalid_values_rejected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError):
        get_settings()
