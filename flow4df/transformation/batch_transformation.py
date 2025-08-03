import flow4df
from typing import Protocol
from pyspark.sql import SparkSession, DataFrame
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

    # def _build_data_frame(
    #     self,
    #     spark: SparkSession,
    #     this_table: flow4df.Table,
    #     data_interval: flow4df.DataInterval
    # ) -> DataFrame:
    #     df = self.transform(spark=spark, this_table=this_table)
