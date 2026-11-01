import pandas as pd
import pytest

from ingestion.validate_data import ValidationError, load_csv, validate_dataframe


def rec(**kw):
    base = dict(sensor_id="TRF-001", location_id="LOC-01", sensor_type="traffic", timestamp="2025-01-01T08:00:00Z",
                latitude="40.7", longitude="-74.0", vehicle_count="50", average_speed="40")
    base.update(kw)
    return base


def run(*rows):
    return validate_dataframe(pd.DataFrame(list(rows)))


def reasons(res):
    return " | ".join(res.rejected["rejection_reason"])


def test_valid_record_accepted():
    r = run(rec())
    assert r.summary["accepted"] == 1 and r.summary["rejected"] == 0


def test_missing_required_column():
    with pytest.raises(ValidationError, match="timestamp"):
        validate_dataframe(pd.DataFrame([{"sensor_id": "a", "location_id": "b", "sensor_type": "traffic"}]))


def test_invalid_timestamp_rejected():
    r = run(rec(timestamp="not-a-date"))
    assert r.summary["accepted"] == 0 and "invalid timestamp" in reasons(r)


def test_duplicate_records_keep_first_only():
    r = run(rec(), rec())
    assert r.summary["accepted"] == 1 and r.summary["duplicates"] == 1


def test_invalid_measurements_rejected():
    r = run(rec(vehicle_count="-5"), rec(timestamp="2025-01-01T08:15:00Z", vehicle_count="abc"),
            rec(sensor_id="ENV-1", sensor_type="environment", timestamp="2025-01-01T08:30:00Z", vehicle_count="",
                average_speed="", temperature="20", humidity="140"))
    assert r.summary["accepted"] == 0
    text = reasons(r)
    assert "vehicle_count out of range" in text and "non-numeric vehicle_count" in text and "humidity out of range" in text


def test_missing_primary_value_rejected_but_missing_optional_kept():
    r = run(rec(vehicle_count=""), rec(timestamp="2025-01-01T08:15:00Z", average_speed=""))
    assert r.summary["accepted"] == 1 and "without vehicle_count" in reasons(r)


@pytest.mark.parametrize("cap", ["0", "-10", "", "x"])
def test_invalid_parking_capacity(cap):
    r = run(rec(sensor_id="PRK-1", sensor_type="parking", parking_capacity=cap, occupied_spaces="10", vehicle_count="", average_speed=""))
    assert r.summary["accepted"] == 0 and r.summary["rejected"] == 1


def test_parking_occupancy_is_computed():
    r = run(rec(sensor_id="PRK-1", sensor_type="parking", parking_capacity="200", occupied_spaces="50", vehicle_count="", average_speed=""))
    assert r.valid["parking_occupancy"].iloc[0] == 25.0


def test_occupied_cannot_exceed_capacity():
    r = run(rec(sensor_id="PRK-1", sensor_type="parking", parking_capacity="10", occupied_spaces="20", vehicle_count="", average_speed=""))
    assert "outside 0..capacity" in reasons(r)


def test_empty_dataset_is_handled():
    r = validate_dataframe(pd.DataFrame(columns=["sensor_id", "location_id", "sensor_type", "timestamp"]))
    assert r.summary["total"] == 0 and r.valid.empty


def test_missing_file_error(tmp_path):
    with pytest.raises(ValidationError, match="not found"):
        load_csv(tmp_path / "nope.csv")


def test_small_test_dataset_validates():
    r = validate_dataframe(load_csv("tests/data/small_test_data.csv"))
    assert r.summary["total"] >= 500 and r.summary["accepted"] > 400
