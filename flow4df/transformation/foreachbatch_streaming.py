from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.streaming.readwriter import DataStreamWriter

import flow4df
from flow4df.table import Transformation, UnitTestTable


class StreamingTransform(Protocol):
    def __call__(
        self, spark: SparkSession, this_table: flow4df.Table,
    ) -> DataFrame:
        ...


class ForeachBatchExecute(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        batch_df: DataFrame,
        epoch_id: int
    ) -> None:
        ...


@dataclass(frozen=True, kw_only=True)
class ForeachBatchStreamingTransformation(Transformation):
    streaming_transform: StreamingTransform
    foreach_batch_execute: ForeachBatchExecute
    default_trigger: flow4df.Trigger
    checkpoint_dir: str = '_checkpoint'

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None
    ) -> StreamingQuery | None:
        _m = 'ForeachBatchStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        # Build the streaming DataFrame from the table (+ upstream tables)
        df = self.streaming_transform(spark=spark, this_table=this_table)

        cp_location = this_table.storage.build_checkpoint_location(
            table_identifier=this_table.table_identifier,
            checkpoint_dir=self.checkpoint_dir
        )
        _trigger = self.default_trigger
        if trigger is not None:
            _trigger = trigger

        def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
            self.foreach_batch_execute(
                spark=spark,
                this_table=this_table,
                batch_df=df,
                epoch_id=epoch_id
            )

        table_id = this_table.table_identifier.table_id
        writer = (
            df
            .writeStream
            .option('checkpointLocation', cp_location)
            .trigger(**_trigger)  # type: ignore
            .queryName(f'foreachbatch_streaming_query_{table_id}')
            .foreachBatch(foreach_batch_function)
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
        del trigger
        _m = 'ForeachBatchStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        ut_table = UnitTestTable.from_table(this_table)
        query = self.run_transformation(
            spark=spark,
            this_table=ut_table,
            trigger={'availableNow': True},
            data_interval=None
        )
        assert query is not None
        query.awaitTermination()
        return None
