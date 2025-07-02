from typing import Protocol, TypeAlias, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter
from flow4df import DataInterval

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]
Reader: TypeAlias = Union[DataFrameReader, DataStreamReader]


class TableFormat(Protocol):
    """Handles Table Format specifics when reading/writing the DataFrame.

    "Table Format" reference:
    https://aws.amazon.com/blogs/big-data/choosing-an-open-table-format-for-your-transactional-data-lake-on-aws/  # noqa
    """

    def configure_reader(self, reader: Reader, location: str) -> Reader:
        """
        Configures the given Reader and returns it.

        Most likely sets:
            - .format()
        """
        ...

    def configure_writer(
        self, writer: Writer, data_interval: DataInterval, location: str
    ) -> Writer:
        """Configures the given Writer and returns it.

        Most likely sets (not an exhaustive list):
            - .format()
            - format specific options, e.g. `mergeSchema` for Delta.
        """
        ...

