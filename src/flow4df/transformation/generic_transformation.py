from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from flow4df.storage.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation.transformation import Transformation
from flow4df.data_interval import DataInterval
from flow4df.common import Trigger


class GenericExecute(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None,
        data_interval: DataInterval | None,
    ) -> None:
        ...


@dataclass(frozen=True, kw_only=True)
class GenericTransformation(Transformation):
    generic_execute: GenericExecute

    def run_transformation(
        self,
        spark: SparkSession,
        this_storage: Storage,
        upstream_storages: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> None:
        self.generic_execute(
            spark=spark,
            this_storage=this_storage,
            upstream_storages=upstream_storages,
            trigger=trigger,
            data_interval=data_interval
        )

    def test_transformation(
        self,
        spark: SparkSession,
        schema: StructType,
        this_storage_stub: Storage,
        upstream_storage_stubs: UpstreamStorages,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None
    ) -> None:
        # TODO: Just call `generic_execute` with the stubs?
        return None
