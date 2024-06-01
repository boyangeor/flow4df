from typing import Protocol, TypeAlias, Union
from dataclasses import dataclass
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.streaming.query import StreamingQuery

from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation.transformation import Transformation
from flow4df.common import OutputMode, Trigger, DataInterval
from flow4df.testing import assertSchemaEqual

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class StreamingTransform(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        upstream_storages: UpstreamStorages,
    ) -> DataFrame:
        ...


@dataclass(frozen=True, kw_only=True)
class StructuredStreamingTransformation(Transformation):
    transform: StreamingTransform
    output_mode: OutputMode
    default_trigger: Trigger
    checkpoint_dir: str = '_checkpoint'

    def _build_data_frame(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages
    ) -> DataFrame:
        df = self.transform(spark=spark, upstream_storages=upstream_storages)
        # Add/replace partitioning columns
        cols_to_add = this_storage.partitioning.build_columns_to_add()
        df = df.withColumns(cols_to_add)
        return df

    def build_writer(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> Writer:
        _m = 'StructuredStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        # Call the Transform to obtain the DataFrame
        df = self._build_data_frame(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=upstream_storages
        )

        cp_location = this_storage.build_checkpoint_location(
            checkpoint_dir=self.checkpoint_dir
        )
        _trigger = self.default_trigger
        if trigger is not None:
            _trigger = trigger

        # Build and configure the Writer
        table_id = this_storage.table_identifier.table_id
        writer = (
            df
            .writeStream
            .outputMode(self.output_mode.name)
            .option('checkpointLocation', cp_location)
            .partitionBy(*this_storage.partitioning.columns)
            .trigger(**_trigger)  # type: ignore
            .queryName(f'streaming_query_{table_id}')
        )
        writer = this_storage.configure_writer(writer)
        return writer

    def start_writer(self, writer: Writer) -> StreamingQuery | None:
        assert isinstance(writer, DataStreamWriter)
        return writer.start()

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_storage: Storage,
        uptream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> None:
        del trigger
        _m = 'StructuredStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        tdf = self._build_data_frame(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=uptream_storage_stubs,
        )
        assertSchemaEqual(actual=tdf.schema, expected=schema)
