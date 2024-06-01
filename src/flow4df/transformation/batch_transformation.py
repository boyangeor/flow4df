from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame

from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation.transformation import Transformation
from flow4df.common import DataInterval

class BatchTransform(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        upstream_storages: UpstreamStorages,
        data_interval: DataInterval
    ) -> DataFrame:
        ...


# @dataclass(frozen=True, kw_only=True)
# class StructuredStreamingTransformation(Transformation):
#     transform: BatchTransform
