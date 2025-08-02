from __future__ import annotations
import logging
import functools
import datetime as dt
from typing import Any, Protocol, Callable
from pyspark.sql import types as T
from dataclasses import dataclass, field, fields
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from flow4df import types
from flow4df import DataInterval, Trigger, TableIdentifier, PartitionSpec
from flow4df import TableFormat, Storage
from flow4df.table_stats import TableStats

log = logging.getLogger(__name__)


def fill_in_spark_session(func: Callable[..., Any]) -> Callable[..., Any]:

    @functools.wraps(func)
    def _wrapper(*args, **kwargs) -> Any:
        no_spark_in_args = len(args) < 2
        no_spark_in_kwargs = kwargs.get('spark') is None
        if no_spark_in_args and no_spark_in_kwargs:
            spark = SparkSession.getActiveSession()
            _m = f'Provide SparkSession to {func.__name__}'
            assert spark is not None, _m
            kwargs['spark'] = spark
        elif not no_spark_in_args:
            _m = 'First positional argument is not a SparkSesssion!'
            assert isinstance(args[1], SparkSession), _m
        elif not no_spark_in_kwargs:
            _m = 'Argument `spark` must be a SparkSession!'
            assert isinstance(kwargs.get('spark'), SparkSession), _m

        return func(*args, **kwargs)

    return _wrapper


@dataclass(frozen=False, kw_only=True)
class Table:
    table_schema: T.StructType = field(repr=False)
    table_identifier: TableIdentifier
    upstream_tables: list[Table] = field(default_factory=list, repr=False)
    transformation: Transformation
    table_format: TableFormat
    storage: Storage
    storage_stub: Storage
    partition_spec: PartitionSpec
    is_active: bool

    @functools.cached_property
    def location(self) -> str:
        return self.storage.build_location(self.table_identifier)

    @functools.cached_property
    def column_types(self) -> dict[str, T.DataType]:
        """<column_name>:<column_type> mapping."""
        return {
            e.name: e.dataType for e in self.table_schema.fields
        }

    def init_table(self, spark: SparkSession) -> None:
        """Initizalize the Table, done by the `table_format`."""
        self.table_format.init_table(
            spark=spark,
            location=self.location,
            table_schema=self.table_schema,
            partition_spec=self.partition_spec
        )

    def init_table_stub(self, spark: SparkSession) -> None:
        """Initizalize the Table stub, used for unit testing."""
        stub_location = self.storage_stub.build_location(self.table_identifier)
        self.table_format.init_table(
            spark=spark,
            location=stub_location,
            table_schema=self.table_schema,
            partition_spec=self.partition_spec
        )

    def _as_df(
        self,
        reader: types.Reader,
        location: str,
        options: dict[str, Any] | None = None
    ) -> DataFrame:
        configured_reader = self.table_format.configure_reader(
            reader=reader, location=location
        )
        if options is not None:
            configured_reader = configured_reader.options(**options)
        return configured_reader.load()

    @fill_in_spark_session
    def as_batch_df(
        self,
        spark: SparkSession | None = None,
        options: dict[str, Any] | None = None,
    ) -> DataFrame:
        assert spark is not None
        return self._as_df(
            reader=spark.read, location=self.location, options=options
        )

    def as_streaming_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None,
    ) -> DataFrame:
        return self._as_df(
            reader=spark.readStream, location=self.location, options=options
        )

    def get_upstream_table(self, name: str) -> Table:
        requested_table = [
            table for table in self.upstream_tables
            if table.table_identifier.name == name
        ]
        _m = 'More than 1 upstream Table found!'
        assert len(requested_table) == 1, _m
        return requested_table[0]

    @fill_in_spark_session
    def run(
        self,
        spark: SparkSession | None = None,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        assert spark is not None
        return self.transformation.run_transformation(
            spark=spark,
            this_table=self,
            trigger=trigger,
            data_interval=data_interval
        )

    def run_table_maintenance(
        self, spark: SparkSession, run_for: dt.timedelta | None = None
    ) -> None:
        self.table_format.run_table_maintenance(
            spark=spark,
            location=self.location,
            partition_spec=self.partition_spec,
            column_types=self.column_types,
            run_for=run_for,
        )
        return None

    def test_transformation(self, spark: SparkSession) -> None:
        self.transformation.test_transformation(spark=spark, this_table=self)

    def calculate_table_stats(self, spark: SparkSession) -> TableStats:
        return self.table_format.calculate_table_stats(spark, self.location)

    def _build_stub_table(self) -> Table:
        table_fields = fields(Table)
        non_storage_fields = [
            field.name for field in table_fields
            if field.name not in {'storage'}
        ]
        init_args = {
            field: getattr(self, field) for field in non_storage_fields
        }
        # Replace the `storage` with `storage_stub`
        init_args['storage'] = self.storage_stub
        return Table(**init_args)


@dataclass(frozen=False, kw_only=True)
class UnitTestTable(Table):
    """TODO."""

    @staticmethod
    def from_table(table: Table) -> UnitTestTable:
        table_fields = fields(Table)
        non_storage_fields = [
            field.name for field in table_fields
            if field.name not in {'storage', 'upstream_tables'}
        ]
        init_args = {
            field: getattr(table, field) for field in non_storage_fields
        }
        init_args['storage'] = table.storage_stub
        init_args['upstream_tables'] = [
            ut._build_stub_table() for ut in table.upstream_tables
        ]
        return UnitTestTable(**init_args)


class Transformation(Protocol):

    def run_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> StreamingQuery | None:
        ...

    def test_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        trigger: Trigger | None = None,
        data_interval: DataInterval | None = None,
    ) -> None:
        ...
