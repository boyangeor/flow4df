from typing import Any
from dataclasses import dataclass


@dataclass(kw_only=True)
class ColumnStats:
    column_name: str
    min_value: Any
    max_value: Any
    row_count: int | None
    null_count: int | None
