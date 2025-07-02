from __future__ import annotations
import logging
import functools
from typing import Any, Protocol
from dataclasses import dataclass, field, asdict, fields
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from flow4df import DataInterval, Trigger, TableIdentifier, PartitionSpec
from flow4df import TableFormat, Storage

log = logging.getLogger(__name__)


@dataclass(frozen=False, kw_only=True)
class Table:
    schema: T.StructType
    table_identifier: TableIdentifier
    upstream_tables: list[Table] = field(default_factory=list)
    transformation: Transformation
    table_format: TableFormat
    storage: Storage
    storage_stub: Storage
    partition_spec: PartitionSpec

    @functools.cached_property
    def location(self) -> str:
        return self.storage.build_location(self.table_identifier)

    def run(
        self,
        spark: SparkSession,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        upstream_tables = UpstreamTables(self.upstream_tables)
        return self.transformation.run_transformation(
            spark=spark,
            this_table=self,
            upstream_tables=upstream_tables,
            trigger=trigger,
            data_interval=data_interval
        )


@dataclass(frozen=False, kw_only=True)
class UnitTestTable(Table):

    @staticmethod
    def from_table(table: Table) -> UnitTestTable:
        table_fields = fields(Table)
        non_storage_fields = [
            field.name for field in table_fields
            if field.name not in {'storage', 'storage_stub'}
        ]
        init_args = {
            field: getattr(table, field) for field in non_storage_fields
        }
        # Replace the `storage` with `storage_stub`
        init_args['storage'] = table.storage_stub
        init_args['storage_stub'] = table.storage_stub
        return UnitTestTable(**init_args)


@dataclass(frozen=True, kw_only=False)
class UpstreamTables:
    tables: list[Table]

    def find_storage(self, catalog: str, schema: str, name: str) -> Table:
        """TODO: implement it."""
        return self.tables[0]


class Transformation(Protocol):

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        upstream_tables: UpstreamTables,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        ...

    def test_transformation(
        self,
        spark: SparkSession,
        schema: T.StructType,
        this_table: Table,
        upstream_tables: UpstreamTables,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> None:
        ...
