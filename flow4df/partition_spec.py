import operator
from dataclasses import dataclass, field


@dataclass(frozen=False, kw_only=True)
class PartitionSpec:
    """Defines physical (file) partitioning of a table.

    Table partitions are usually set via `.partitionBy('col1', 'col2')` when
    writing. This class aims to:
      - Provide more context by splitting the partitioning columns in two
        lists. The `time_non_monotonic` columns have a relatively fixed set of
        values e.g 'country'. The 'time_monotonic_increasing' columns have
        values that increase as new data arrives e.g. 'event_date'. This
        information should be easily accessible, we should not have to browse
        through the underlying files.
      - Catch errors early on, before the SparkSession is initialized.

    Parameters
    ----------
    time_non_monotonic : list of str
        Columns with fixed set of possible values.
    time_monotonic_increasing : list of str
        Columns with infinite set of possible values that increase with time.

    Attributes
    ----------
    time_non_monotonic : list of str
        Columns with fixed set of possible values.
    time_monotonic_increasing : list of str
        Columns with infinite set of possible values that increase with time.
    columns : list of str
        `time_non_monotonic` + `time_monotonic_increasing` for easier access.
    column_count : int
         The number of columns, len(`columns`)


    Examples
    --------
    >>> from flow4df import PartitionSpec
    >>> partition_spec = PartitionSpec(
    ...     time_non_monotonic=['country'],
    ...     time_monotonic_increasing=['event_year', 'event_date'])
    >>> partition_spec.columns
    ['country', 'event_year', 'event_date']
    """
    time_non_monotonic: list[str]
    time_monotonic_increasing: list[str]
    columns: list[str] = field(default_factory=list, init=False, repr=True)
    column_count: int = field(init=False, repr=True)

    def __post_init__(self) -> None:
        self.columns = operator.add(
            self.time_non_monotonic, self.time_monotonic_increasing,
        )
        self.column_count = len(self.columns)
