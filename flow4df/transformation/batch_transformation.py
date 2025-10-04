import datetime as dt
from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.query import StreamingQuery

import flow4df
from flow4df.table import Transformation


class BatchTransform(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        data_interval: flow4df.DataInterval
    ) -> DataFrame:
        ...


@dataclass(frozen=True, kw_only=True)
class BatchTransformation(Transformation):
    transform: BatchTransform
    output_mode: flow4df.enums.OutputMode

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> StreamingQuery | None:
        _m = 'BatchTransformation should not receive `trigger`!'
        assert trigger is None, _m
        _m = 'BatchTransformation MUST receive `data_interval`!'
        assert data_interval is not None, _m

        transformed_df = self.transform(
            spark=spark, this_table=this_table, data_interval=data_interval
        )
        # Build and configure the Writer
        writer = this_table.table_format.build_batch_writer(
            df=transformed_df,
            table_identifier=this_table.table_identifier,
            output_mode=self.output_mode,
            partition_spec=this_table.partition_spec
        )
        writer = this_table.table_format.configure_writer(
            writer=writer,
            location=this_table.location,
            data_interval=data_interval
        )
        return self.start_writer(writer=writer, output_mode=self.output_mode)
        # assert isinstance(writer, DataFrameWriter)
        # return writer.save()

    def test_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None
    ) -> None:
        """
        Test if the `transform` is logically correct. Also assert
        it produces a DataFrame with the expected schema.
        """
        _m = 'BatchTransformation should not receive `trigger`!'
        assert trigger is None, _m
        _m = 'BatchTransformation MUST receive `data_interval`!'
        assert data_interval is not None, _m

        transformed_df = self.transform(
            spark=spark, this_table=this_table, data_interval=data_interval
        )
        flow4df.tools.schema.assert_schemas_equivalent(
            spark=spark,
            actual=transformed_df.schema,
            expected=this_table.table_schema,
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
