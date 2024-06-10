import functools
from dataclasses import dataclass
from pendulum.datetime import DateTime


@dataclass(frozen=True, kw_only=True)
class DataInterval:
    start: DateTime
    end: DateTime

    @functools.cached_property
    def start_unix_ts_seconds(self) -> int:
        return int(self.start.timestamp())

