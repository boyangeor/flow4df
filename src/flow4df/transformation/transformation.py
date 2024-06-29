from typing import Protocol, TypeAlias, Union
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.streaming.query import StreamingQuery
from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.data_interval import DataInterval
from flow4df.common import Trigger


Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class Transformation(Protocol):
    def run_transformation(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        ...

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_storage: Storage,
        upstream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> None:
        ...
