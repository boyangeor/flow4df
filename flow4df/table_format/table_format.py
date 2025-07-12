import datetime as dt
from typing import Protocol
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DataType

from flow4df import types
from flow4df import DataInterval, PartitionSpec


class TableFormat(Protocol):
    """Handles Table Format specifics when reading/writing the DataFrame.

    "Table Format" reference:
    https://aws.amazon.com/blogs/big-data/choosing-an-open-table-format-for-your-transactional-data-lake-on-aws/  # noqa
    """

    def configure_reader(
        self, reader: types.Reader, location: str
    ) -> types.Reader:
        """
        Configures the given Reader and returns it.

        Most likely sets:
            - .format()
        """
        ...

    def configure_writer(
        self, writer: types.Writer, data_interval: DataInterval, location: str
    ) -> types.Writer:
        """Configures the given Writer and returns it.

        Most likely sets (not an exhaustive list):
            - .format()
            - format specific options, e.g. `mergeSchema` for Delta.
        """
        ...

    def init_table(
        self,
        spark: SparkSession,
        location: str,
        schema: StructType,
        partition_spec: PartitionSpec
    ) -> None:
        """Initializes the table e.g. creates an empty Delta table."""
        ...

    def run_table_maintenance(
        self,
        spark: SparkSession,
        location: str,
        partition_spec: PartitionSpec,
        column_types: dict[str, DataType],
        run_for: dt.timedelta | None = None,
    ) -> None:
        """Most likely does (not an exhaustive list):
            - compaction of small files
            - vacuuming stale files
            - optimizing file layout e.g. zOrderBy
            - cleaning lock files e.g. in _delta_log/.tmp/
        """
        ...
