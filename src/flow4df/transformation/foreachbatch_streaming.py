from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.streaming.readwriter import DataStreamWriter

from flow4df.common import Trigger
from flow4df.data_interval import DataInterval
from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation.transformation import Transformation


class BuildStreamingDataFrame(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        upstream_storages: UpstreamStorages,
    ) -> DataFrame:
        ...


class ForeachBatchExecute(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        this_storage: Storage,
        batch_df: DataFrame,
        epoch_id: int
    ) -> None:
        ...


@dataclass(frozen=True, kw_only=True)
class ForeachBatchStreamingTransformation(Transformation):
    build_streaming_df: BuildStreamingDataFrame
    foreach_batch_execute: ForeachBatchExecute
    default_trigger: Trigger
    checkpoint_dir: str = '_checkpoint'

    def run_transformation(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> StreamingQuery | None:
        _m = 'ForeachBatchStreaming should not receive `data_interval`!'
        assert data_interval is None, _m

        # Build the streaming DataFrame from the upstream storages
        df = self.build_streaming_df(
            spark=spark,
            upstream_storages=upstream_storages
        )
        cp_location = this_storage.build_checkpoint_location(
            checkpoint_dir=self.checkpoint_dir
        )
        _trigger = self.default_trigger
        if trigger is not None:
            _trigger = trigger

        # foreach_batch_function = functools.partial(
        #     self.foreach_batch_execute, this_storage=this_storage
        # )
        def foreach_batch_function(df: DataFrame, epoch_id: int):
            self.foreach_batch_execute(
                spark=spark,
                this_storage=this_storage,
                batch_df=df,
                epoch_id=epoch_id
            )

        table_id = this_storage.table_identifier.table_id
        writer = (
            df
            .writeStream
            .option('checkpointLocation', cp_location)
            .queryName(f'foreachbatch_streaming_query_{table_id}')
            .trigger(**_trigger)  # type: ignore
            .foreachBatch(foreach_batch_function)
        )
        assert isinstance(writer, DataStreamWriter)
        return writer.start()

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_storage: Storage,
        upstream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> None:
        del schema, trigger, data_interval
        query = self.run_transformation(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=upstream_storage_stubs,
            trigger={'availableNow': True},
        )
        assert query is not None
        query.awaitTermination()
