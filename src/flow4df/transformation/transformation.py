from typing import Protocol, TypeAlias, Union
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.streaming.query import StreamingQuery
from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.common import Trigger, DataInterval

Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


class Transformation(Protocol):
    def build_writer(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> Writer:
        ...

    def start_writer(self, writer: Writer) -> StreamingQuery | None:
        ...

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_storage: Storage,
        uptream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> None:
        ...
