from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming.query import StreamingQuery
from flow4df.common import OutputMode, Trigger

from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation.transformation import Transformation
from flow4df.data_interval import DataInterval
from flow4df.testing import assertSchemaEqual


class BatchTransform(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        upstream_storages: UpstreamStorages,
        data_interval: DataInterval
    ) -> DataFrame:
        ...


@dataclass(frozen=True, kw_only=True)
class BatchTransformation(Transformation):
    transform: BatchTransform
    output_mode: OutputMode
    is_pure: bool

    def _build_data_frame(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        data_interval: DataInterval
    ) -> DataFrame:
        df = self.transform(
            spark=spark,
            upstream_storages=upstream_storages,
            data_interval=data_interval
        )
        # Add/replace partitioning columns
        cols_to_add = this_storage.partitioning.build_columns_to_add()
        df = df.withColumns(cols_to_add)
        return df

    def run_transformation(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> StreamingQuery | None:
        _m = 'BatchTransformation should not receive `trigger`!'
        assert trigger is None, _m
        _m = 'BatchTransformation MUST receive `data_interval`!'
        assert data_interval is not None, _m
        # Call the Transform to obtain the DataFrame
        df = self._build_data_frame(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=upstream_storages,
            data_interval=data_interval
        )
        # Build and configure the Writer
        writer = (
            df
            .write
            .mode(self.output_mode.name)
            .partitionBy(*this_storage.partitioning.columns)
        )
        writer = this_storage.configure_writer(
            writer=writer, data_interval=data_interval
        )
        assert isinstance(writer, DataFrameWriter)
        return writer.save()

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_storage: Storage,
        upstream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> None:
        """
        Test if the `transform` is valid for Structured Streaming. Also assert
        it produces a DataFrame with the expected schema.
        """
        _m = 'BatchTransformation should not receive `trigger`!'
        assert trigger is None, _m
        _m = 'BatchTransformation MUST receive `data_interval`!'
        assert data_interval is not None, _m

        tdf = self._build_data_frame(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=upstream_storage_stubs,
            data_interval=data_interval,
        )
        assertSchemaEqual(actual=tdf.schema, expected=schema)
