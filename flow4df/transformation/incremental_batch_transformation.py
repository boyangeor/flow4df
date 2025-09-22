import datetime as dt
from typing import Protocol
from dataclasses import dataclass

from pyspark.sql import SparkSession
import flow4df
from flow4df.transformation.batch_transformation import BatchTransformation


class GetUpstreamWatermak(Protocol):
    def __call__(
        self,
        spark: SparkSession,
        upstream_tables: list[flow4df.Table]
    ) -> dt.datetime:
        ...


@dataclass(frozen=True, kw_only=True)
class IncrementalBatchTransformation(BatchTransformation):
    get_upstream_watermark: GetUpstreamWatermak
    start: dt.datetime
    min_interval_length: dt.timedelta
    max_interval_length: dt.timedelta

    def build_next_data_interval(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
    ) -> flow4df.DataInterval | None:
        reached = self.start
        if not this_table.is_initialized_only(spark):
            table_format = this_table.table_format
            last_data_interval = table_format.get_last_batch_data_interval(
                spark=spark,
                location=this_table.location,
                table_identifier=this_table.table_identifier
            )
            reached = last_data_interval.end

        upstream_wm = self.get_upstream_watermark(
            spark=spark, upstream_tables=this_table.upstream_tables
        )
        to_catch_up = upstream_wm - reached
        if to_catch_up < self.min_interval_length:
            return None

        next_interval_length = min(to_catch_up, self.max_interval_length)
        return flow4df.DataInterval(
            start=reached,
            end=reached + next_interval_length
        )
