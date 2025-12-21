import pytest
import datetime as dt
from flow4df import DataInterval


def test_init() -> None:
    di = DataInterval(
        start=dt.datetime(2025, 6, 29, 0, tzinfo=dt.UTC),
        end=dt.datetime(2025, 6, 29, 1, tzinfo=dt.UTC),
    )
    assert di.start_unix_ts_seconds == 1751155200.0
    assert di.end_unix_ts_seconds == 1751158800.0
    return None


def test_from_unix_timestamps() -> None:
    _start = 1751155200.0
    di1 = DataInterval.from_unix_timestamps(
        start_unix_seconds=_start,
        end_unix_seconds=_start + 3600
    )
    di2 = DataInterval(
        start=dt.datetime(2025, 6, 29, 0, tzinfo=dt.UTC),
        end=dt.datetime(2025, 6, 29, 1, tzinfo=dt.UTC),
    )
    assert di1 == di2


def test_from_iso_formatted_timestamps() -> None:
    di1 = DataInterval.from_iso_formatted_timestamps(
        start_iso_timestamp='1970-01-01T10:00:00+02:00',
        end_iso_timestamp='1970-01-01T12:00:00+02:00',
    )
    di2 = DataInterval.from_iso_formatted_timestamps(
        start_iso_timestamp='1970-01-01T08:00:00+00:00',
        end_iso_timestamp='1970-01-01T10:00:00+00:00',
    )
    assert di1 == di2
    return None


def test_ambiguous_timestamp() -> None:
    with pytest.raises(ValueError):
        DataInterval.from_iso_formatted_timestamps(
            start_iso_timestamp='1970-01-01 10:00:00',
            end_iso_timestamp='1970-01-02 10:00:00',
        )
    return None
