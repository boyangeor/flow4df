"""
Commonly used classes, protocols, enums.
"""
from enum import Enum
from dataclasses import dataclass
from collections.abc import Callable
from typing import TypedDict, Protocol
from pyspark.sql import Column, DataFrame


@dataclass(frozen=True, kw_only=False)
class NamedColumn:
    """`pyspark.sql.Column` that can be initialized later on.

    Necessary because Column instances cannot be created without SparkSession.
    The Column initialization can be delayed via thunk:
    https://en.wikipedia.org/wiki/Thunk

    """
    name: str
    column_thunk: Callable[[], Column]


class Trigger(TypedDict, total=False):
    availableNow: bool
    processingTime: str


class Transformation(Protocol):
    def __call__(self, df: DataFrame) -> DataFrame:
        ...


class OutputMode(Enum):
    append = 1     # streaming and batch
    update = 2     # streaming
    complete = 3   # streaming
    error = 4      # batch
    overwrite = 5  # batch
    ignore = 6     # batch
