from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class TableStats:
    file_count: int
    row_count: int
    size_gib: float
