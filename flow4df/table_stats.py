import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class TableStats:
    file_count: int
    row_count: int
    size_gib: float
    min_file_ts: dt.datetime
    max_file_ts: dt.datetime
    min_file_size_mib: float
    max_file_size_mib: float
    avg_file_size_mib: float
