from dataclasses import dataclass
from typing import TypeAlias, Union
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter
from flow4df import TableFormat
from flow4df import DataInterval

TABLE_FORMAT_NAME = 'delta'

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]
Reader: TypeAlias = Union[DataFrameReader, DataStreamReader]


@dataclass(frozen=True, kw_only=True)
class DeltaTableFormat(TableFormat):
    merge_schema: bool = True

    def configure_reader(self, reader: Reader, location: str) -> Reader:
        return (
            reader
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
        )

    def configure_writer(
        self, writer: Writer, location: str, data_interval: DataInterval,
    ) -> Writer:
        return (
            writer
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
            .option('mergeSchema', self.merge_schema)
        )
