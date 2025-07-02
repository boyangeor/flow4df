from typing import Protocol, TypeAlias, Union
from dataclasses import dataclass
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.streaming.query import StreamingQuery

from flow4df import Table, UpstreamTables, Transformation
from flow4df import Trigger, DataInterval
from flow4df.enums import OutputMode

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class StreamingTransform(Protocol):
    def __call__(
        self, spark: SparkSession, upstream_tables: UpstreamTables,
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
        this_table: Table,
        upstream_tables: UpstreamTables
    ) -> DataFrame:
        df = self.transform(spark=spark, upstream_tables=upstream_tables)
        return df

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        upstream_tables: UpstreamTables,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> StreamingQuery | None:
        _m = 'StructuredStreaming should not receive `data_interval`!'
        assert data_interval is None, _m
        # Call the Transform to obtain the DataFrame
        df = self._build_data_frame(
            spark=spark,
            this_table=this_table,
            upstream_tables=upstream_tables,
        )
        cp_location = this_table.storage.build_checkpoint_location(
            table_identifier=this_table.table_identifier,
            checkpoint_dir=self.checkpoint_dir
        )
        _trigger = self.default_trigger
        if trigger is not None:
            _trigger = trigger

        # Build and configure the Writer
        table_id = this_table.table_identifier.table_id
        writer = (
            df
            .writeStream
            .outputMode(self.output_mode.name)
            .option('checkpointLocation', cp_location)
            .partitionBy(*this_table.partition_spec.columns)
            .trigger(**_trigger)  # type: ignore
            .queryName(f'streaming_query_{table_id}')
        )
        writer = this_table.table_format.configure_writer(
            writer, data_interval=data_interval, location=this_table.location
        )
        assert isinstance(writer, DataStreamWriter)
        return writer.start()

    # def test_transformation(
    #     self,
    #     spark: SparkSession,
    #     schema: T.StructType,
    #     this_: Storage,
    #     upstream_storage_stubs: UpstreamStorages,
    #     trigger: Trigger | None = None,
    #     data_interval: DataInterval | None = None
    # ) -> None:
    #     """
    #     Test if the `transform` is valid for Structured Streaming. Also assert
    #     it produces a DataFrame with the expected schema.
    #     """
    #     del trigger
    #     _m = 'StructuredStreaming should not receive `data_interval`!'
    #     assert data_interval is None, _m

    #     tdf = self._build_data_frame(
    #         spark=spark,
    #         this_storage=this_storage_stub,
    #         upstream_storages=upstream_storage_stubs,
    #     )
    #     assertSchemaEqual(actual=tdf.schema, expected=schema)
