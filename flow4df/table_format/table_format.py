import datetime as dt
from typing import Protocol
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, DataType

from flow4df import types, enums
from flow4df import DataInterval, PartitionSpec, TableIdentifier
from flow4df.table_stats import TableStats
from flow4df.column_stats import ColumnStats


class TableFormat(Protocol):
    """Handles Table Format specifics when reading/writing the DataFrame.

    "Table Format" reference:
    https://aws.amazon.com/blogs/big-data/choosing-an-open-table-format-for-your-transactional-data-lake-on-aws/  # noqa
    """

    def build_batch_writer(
        self,
        df: DataFrame,
        table_identifier: TableIdentifier,
        output_mode: enums.OutputMode,
        partition_spec: PartitionSpec
    ) -> types.Writer:
        """Builds the required Writer."""
        ...

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
        self, writer: types.Writer,
        location: str,
        data_interval: DataInterval | None,
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
        table_identifier: TableIdentifier,
        table_schema: StructType,
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

    def calculate_table_stats(
        self, spark: SparkSession, location: str
    ) -> TableStats:
        """Calculate basic stats for the table."""
        ...

    def get_column_stats(
        self,
        spark: SparkSession,
        location: str,
        column_types: dict[str, DataType],
        column_name: str,
        table_identifier: TableIdentifier,
    ) -> ColumnStats:
        """
        Calculate basic stats for the column.

        Ideally the data files should not be queried, only the meta data.
        """
        ...

    def is_initialized_only(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
    ) -> bool:
        """Check if the table is only initialized, without any appends."""
        ...

    def get_last_batch_data_interval(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
    ) -> DataInterval:
        """Read the DataInterval of the last batch run."""
        ...

    def configure_session(
        self,
        spark: SparkSession,
        table_identifier: TableIdentifier,
        catalog_location: str,
    ) -> None:
        """
        Configure the Spark Session with properties required by the
        TableFormat implementation.
        """
        ...
