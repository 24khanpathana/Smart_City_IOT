import pytest

from ingestion.generate_sensor_data import GeneratorConfig, build_dataframe


def test_same_seed_is_reproducible():
    cfg = GeneratorConfig(sensors=8, locations=2, records=300, end="2025-01-03")
    assert build_dataframe(cfg).equals(build_dataframe(cfg))


def test_different_seed_differs():
    a = build_dataframe(GeneratorConfig(sensors=8, locations=2, records=300, end="2025-01-03", seed=1))
    b = build_dataframe(GeneratorConfig(sensors=8, locations=2, records=300, end="2025-01-03", seed=2))
    assert not a.equals(b)


def test_all_sensor_types_and_rush_hour_pattern():
    df = build_dataframe(GeneratorConfig(sensors=12, locations=3, records=3000, end="2025-01-10", invalid_rate=0, missing_rate=0, anomaly_rate=0))
    assert set(df.sensor_type) == {"traffic", "environment", "parking", "waste"}
    t = df[df.sensor_type == "traffic"].copy()
    t["h"] = t.timestamp.str[11:13].astype(int)
    assert t[t.h.isin([8, 17])].vehicle_count.mean() > 2 * t[t.h.isin([2, 3])].vehicle_count.mean()


def test_invalid_config():
    with pytest.raises(ValueError):
        GeneratorConfig(sensors=2).validate()
    with pytest.raises(ValueError):
        GeneratorConfig(sensor_types=("radar",)).validate()
