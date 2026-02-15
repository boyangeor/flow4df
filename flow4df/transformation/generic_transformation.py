from typing import Protocol
from dataclasses import dataclass
from pyspark.sql import SparkSession

import flow4df
from flow4df.table import Transformation


class GenericExecute(Protocol):
    def __call__(
        self, spark: SparkSession, this_table: flow4df.Table,
    ) -> None:
        ...


@dataclass(frozen=True, kw_only=True)
class GenericTransformation(Transformation):
    generic_execute: GenericExecute

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        del trigger, data_interval
        self.generic_execute(spark=spark, this_table=this_table)
        return None

    def test_transformation(
        self,
        spark: SparkSession,
        this_table: flow4df.Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        # TODO: Just call `generic_execute` with the stubs?
        return None
