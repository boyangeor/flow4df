import operator
from dataclasses import dataclass
from pyspark.sql import Column
from pyspark.sql import types as T

from flow4df import utils
from flow4df.common import NamedColumn


def _get_column_names(named_columns: list[NamedColumn]) -> list[str]:
    return [e.name for e in named_columns]


def _get_columns_to_add(named_columns: list[NamedColumn]) -> dict[str, Column]:
    return {e.name: e.column_thunk() for e in named_columns}


@dataclass(frozen=False, kw_only=True)
class Partitioning:
    """Defines physical (file) partitioning of a table.

    Table partitions are usually set via `.partitionBy('col1', 'col2')` when
    writing. This class aims to:
      - Provide more context by splitting the partitioning columns in two
        lists. The `time_non_monotonic` columns have a relatively fixed set of
        values e.g 'country'. The 'time_monotonic_increasing' columns have
        values that increase as new data arrives e.g. 'event_date'. This
        information should be easily accessible, we should not have to browse
        through the underlying files.
      - Provide a mechanisim to automatically add partition columns if they
        were not added in the transformation.
      - Catch errors early on, before the SparkSession is initialized.

    Parameters
    ----------
    time_non_monotonic
        Columns with fixed set of possible values.
    time_monotonic_increasing
        Columns with infinite set of possible values that increase with time.
    """
    time_non_monotonic: list[NamedColumn]
    time_monotonic_increasing: list[NamedColumn]

    def __post_init__(self) -> None:
        self.columns: list[str] = operator.add(
            _get_column_names(self.time_non_monotonic),
            _get_column_names(self.time_monotonic_increasing),
        )
        self.n_columns = len(self.columns)

    def build_columns_to_add(self) -> dict[str, Column]:
        return operator.or_(
            _get_columns_to_add(self.time_non_monotonic),
            _get_columns_to_add(self.time_monotonic_increasing),
        )

    def build_log_columns(self, pv_map: Column) -> dict[str, Column]:
        return {c: pv_map.getItem(c) for c in self.columns}

    def assert_schema_compatibility(self, schema: T.StructType) -> None:
        missing_cols = [c for c in self.columns if c not in schema.names]
        _missing = ', '.join(missing_cols)
        _m = f'\nPartitioning columns not in schema: [{_missing}]\n\n'
        _m += utils.prettify_schema(schema)
        assert len(missing_cols) == 0, _m
