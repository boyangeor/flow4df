from __future__ import annotations
import types
import inspect
import logging
import functools
from typing import Any
from dataclasses import dataclass, field, asdict
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from flow4df.storage import Storage
from flow4df.upstream_storages import UpstreamStorages
from flow4df.transformation import Transformation
from flow4df.data_interval import DataInterval
from flow4df.common import Trigger

log = logging.getLogger(__name__)


@dataclass(frozen=False, kw_only=True)
class TableNode:
    schema: T.StructType
    upstream_table_nodes: list[TableNode] = field(default_factory=list)
    transformation: Transformation
    storage: Storage
    storage_stub: Storage
    documentation: str | None = None

    def __post_init__(self) -> None:
        _m = '`storage` and `storage_stub` must have the same partitioning!'
        assert self.storage.partitioning == self.storage_stub.partitioning, _m
        pass

    def run(
        self,
        spark: SparkSession,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        upstream_storages = UpstreamStorages(
            storages=[tn.storage for tn in self.upstream_table_nodes]
        )
        return self.transformation.run_transformation(
            spark=spark,
            this_storage=self.storage,
            upstream_storages=upstream_storages,
            trigger=trigger,
            data_interval=data_interval,
        )

    def dry_run(
        self,
        spark: SparkSession,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> None:
        _m = f'Dry Run for `{self.table_id}` ...'
        log.warning(_m)
        upstream_storage_stubs = UpstreamStorages(
            storages=[tn.storage_stub for tn in self.upstream_table_nodes]
        )
        self.transformation.test_transformation(
            spark=spark,
            schema=self.schema,
            this_storage_stub=self.storage_stub,
            upstream_storage_stubs=upstream_storage_stubs,
            trigger=trigger,
            data_interval=data_interval,
        )
        log.warning('Dry Run complete.')

    def init_table_storage(self, spark: SparkSession) -> None:
        """Done by the `storage`."""
        self.storage.init_storage(spark=spark, schema=self.schema)

    def init_table_storage_stub(self, spark: SparkSession) -> None:
        """Done by the `storage_stub`."""
        self.storage_stub.init_storage(spark=spark, schema=self.schema)

    def run_maintenance(self, spark: SparkSession) -> None:
        """Should be done by the Storage."""
        self.storage.run_storage_maintenance(
            spark=spark, column_types=self.column_types
        )

    def as_batch_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None,
    ) -> DataFrame:
        df = self.storage.build_batch_df(spark=spark, options=options)
        assert not df.isStreaming, 'Must return a batch DataFrame!'
        return df

    def as_streaming_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None,
    ) -> DataFrame:
        df = self.storage.build_streaming_df(spark=spark, options=options)
        assert df.isStreaming, 'Must return a streaming DataFrame!'
        return df

    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def database(self) -> str:
        return self.storage.table_identifier.database

    @property
    def name(self) -> str:
        return self.storage.table_identifier.name

    @property
    def version(self) -> int:
        return self.storage.table_identifier.version

    @functools.cached_property
    def table_id(self) -> str:
        return self.storage.table_identifier.table_id

    @functools.cached_property
    def column_types(self) -> dict[str, T.DataType]:
        """<column_name>:<column_type> mapping."""
        return {
            e.name: e.dataType for e in self.schema.fields
        }

    @staticmethod
    def list_table_nodes(
        database_package: types.ModuleType
    ) -> list[TableNode]:
        """List all TableNode instances exposed in a module/package."""
        members = inspect.getmembers(
            object=database_package,
            predicate=lambda e: isinstance(e, TableNode)
        )
        return [table_node_instance for _, table_node_instance in members]

    @functools.cached_property
    def column(self):
        from dataclasses import make_dataclass
        ColumnClass = make_dataclass(
            'ColumnClass', fields=[(n, str) for n in self.schema.names]
        )
        _cols = {n: n for n in self.schema.names}
        return ColumnClass(**_cols)
