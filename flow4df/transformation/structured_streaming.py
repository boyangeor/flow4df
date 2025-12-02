from typing import Protocol, TypeAlias, Union
from dataclasses import dataclass
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.streaming.query import StreamingQuery

import flow4df
from flow4df.table import Transformation

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class StreamingTransform(Protocol):
    def __call__(
        self, spark: SparkSession, this_table: flow4df.Table,
    ) -> DataFrame:
        ...


@dataclass(frozen=True, kw_only=True)
class StructuredStreamingTransformation(Transformation):
    transform: StreamingTransform
    output_mode: flow4df.enums.OutputMode
    default_trigger: flow4df.Trigger
    checkpoint_dir: str = '_checkpoint'

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> StreamingQuery | None:
        _m = 'StructuredStreaming should not receive `data_interval`!'
        assert data_interval is None, _m
        # Call the Transform to obtain the DataFrame
        df = self.transform(spark=spark, this_table=this_table)

        if this_table.partition_spec.time_bucketing_column is not None:
            df = df.withColumn('_time_bucket', F.lit(0))

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
            .partitionBy(*this_table.partition_spec.partition_by)
            .trigger(**_trigger)  # type: ignore
            .queryName(f'streaming_query_{table_id}')
        )
        writer = this_table.table_format.configure_writer(
            writer, location=this_table.location, data_interval=None
        )
        assert isinstance(writer, DataStreamWriter)
        return writer.start()

    def test_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None
    ) -> None:
        """
        Test if the `transform` is valid for Structured Streaming. Also assert
        it produces a DataFrame with the expected schema.
        """
        del trigger
        _m = 'StructuredStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        tdf = self.transform(spark=spark, this_table=this_table)
        flow4df.tools.schema.assert_schemas_equivalent(
            spark=spark, actual=tdf.schema, expected=this_table.table_schema,
        )
        return None

    def build_next_data_interval(
        self,
        # upstream_watermark: dt.datetime,
        # last_data_interval: flow4df.DataInterval
        spark: SparkSession,
        this_table: flow4df.Table,
    ) -> flow4df.DataInterval | None:
        del spark, this_table
        _m = (
            '`build_next_data_interval` not applicable for BatchTransformation'
            '!'
        )
        raise NotImplementedError(_m)
