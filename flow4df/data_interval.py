from __future__ import annotations
import json
import datetime as dt
from dataclasses import dataclass, asdict


@dataclass(kw_only=True)
class DataInterval:
    """Represent a TZ aware interval where `start` < `end`.

    Parameters
    ----------
    start : datetime
        Start of the interval
    end : datetime
        End of the interval

    Attributes
    ----------
    start : datetime
        Start of the interval
    end : datetime
        End of the interval
    """
    start: dt.datetime
    end: dt.datetime

    def __post_init__(self) -> None:
        _m = '`start` must be before `end`!'
        assert self.start < self.end, _m

        _m = 'Timestamp must be TZ aware!'
        assert self._is_tz_aware(self.start), _m
        assert self._is_tz_aware(self.end), _m

    @property
    def start_unix_ts_seconds(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_unix_ts_seconds(self) -> int:
        return int(self.end.timestamp())

    def as_dict(self) -> dict[str, dt.datetime]:
        return asdict(self)

    @staticmethod
    def from_json(json_str: str) -> DataInterval:
        d = json.loads(json_str)
        return DataInterval(
            start=dt.datetime.fromisoformat(d['start']),
            end=dt.datetime.fromisoformat(d['end']),
        )

    @staticmethod
    def _is_tz_aware(d: dt.datetime) -> bool:
        return (d.tzinfo is not None) and (d.tzinfo.utcoffset is not None)

    @staticmethod
    def from_unix_timestamps(
        start_unix_seconds: float, end_unix_seconds: float
    ) -> DataInterval:
        """Create DataInterval from UNIX timestamps.

        Parameters
        ----------
        start_unix_seconds : float
            The start of the interval as seconds since the Epoch
        end_unix_seconds : float
            The end of the interval as seconds since the Epoch

        Examples
        --------
        >>> from flow4df import DataInterval
        >>> some_interval = DataInterval.from_unix_timestamps(
        ...     start_unix_seconds=1751155200.0,
        ...     end_unix_seconds=1751155200.0 + 7200)
        >>> some_interval.start
        datetime.datetime(2025, 6, 29, 0, 0, tzinfo=datetime.timezone.utc)
        """
        start = dt.datetime.fromtimestamp(start_unix_seconds, tz=dt.UTC)
        end = dt.datetime.fromtimestamp(end_unix_seconds, tz=dt.UTC)
        return DataInterval(start=start, end=end)

    @staticmethod
    def _parse_iso_timestamp(iso_timestamp: str) -> dt.datetime:
        _ts = dt.datetime.fromisoformat(iso_timestamp)
        if not DataInterval._is_tz_aware(_ts):
            _ts = _ts.replace(tzinfo=dt.UTC)

        return _ts

    @staticmethod
    def from_iso_formatted_timestamps(
        start_iso_timestamp: str, end_iso_timestamp: str
    ) -> DataInterval:
        """Create DataInterval from ISO 8601 formatted timestamps.

        Parameters
        ----------
        start_iso_timestamp : str
            The start of the interval as ISO timestamp.
        end_iso_timestamp : str
            The end of the interval as ISO timestamp.

        Examples
        --------
        >>> from flow4df import DataInterval
        >>> some_interval = DataInterval.from_iso_formatted_timestamps(
        ...     start_iso_timestamp='2025-06-29T00:00:00Z',
        ...     end_iso_timestamp='2025-06-29T02:00:00Z')
        >>> some_interval.end
        datetime.datetime(2025, 6, 29, 2, 0, tzinfo=datetime.timezone.utc)
        """
        start = DataInterval._parse_iso_timestamp(start_iso_timestamp)
        end = DataInterval._parse_iso_timestamp(end_iso_timestamp)
        return DataInterval(start=start, end=end)
