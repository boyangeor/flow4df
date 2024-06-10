from typing import Protocol, TypeAlias, Union, Any
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter

from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend import StorageBackend
from flow4df.partitioning import Partitioning
from flow4df.data_interval import DataInterval

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class Storage(Protocol):
    """Handles Table Format specifics when reading/writing the DataFrame.

    "Table Format" reference:
    https://aws.amazon.com/blogs/big-data/choosing-an-open-table-format-for-your-transactional-data-lake-on-aws/  # noqa
    """
    table_identifier: TableIdentifier
    storage_backend: StorageBackend
    partitioning: Partitioning

    def configure_writer(
        self, writer: Writer, data_interval: DataInterval | None = None,
    ) -> Writer:
        """Configures the given Writer and returns it.

        Most likely sets (not an exhaustive list):
            - .format()
            - .option('path', '<location>')
        """
        ...

    def build_checkpoint_location(self, checkpoint_dir: str) -> str:
        """Builds a `checkpointLocation` for the given  `checkpoint_dir`.

        The `flow4df.transformation.Transformation` implementation decides on
        how/if to use this.

        Reference:
        https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing  # noqa
        """
        ...

    def init_storage(
        self, spark: SparkSession, schema: T.StructType
    ) -> None:
        """Initializes the storage e.g. creates an empty Delta table."""
        ...

    def run_storage_maintenance(
        self, spark: SparkSession, column_types: dict[str, T.DataType]
    ) -> None:
        """Most likely does (not an exhaustive list):
            - compaction of small files
            - vacuuming stale files
            - optimizing file layout e.g. zOrderBy
            - cleaning lock files e.g. in _delta_log/.tmp/
        """
        ...

    def build_batch_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        """Builds a batch DataFrame (.isStreaming = False)."""
        ...

    def build_streaming_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        """Builds a streaming DataFrame (.isStreaming = True)."""
        ...

    def build_storage_stats(self, spark: SparkSession) -> DataFrame:
        # TODO: should return a concrete data structure, DataFrame is too
        # abstract
        ...

    @property
    def location(self) -> str:
        ...
