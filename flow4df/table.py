from __future__ import annotations
import inspect
import difflib
import logging
import functools
import datetime as dt
from types import ModuleType
from typing import Any, Protocol, Callable
from pyspark.sql import types as T
from dataclasses import dataclass, field, fields
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery

import flow4df
from flow4df import types
from flow4df.table_format.table_format import TableFormat
from flow4df.storage.storage import Storage
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
    table_identifier: flow4df.TableIdentifier
    upstream_tables: list[Table] = field(default_factory=list, repr=False)
    transformation: Transformation
    table_format: TableFormat
    storage: Storage
    storage_stub: Storage
    partition_spec: flow4df.PartitionSpec
    is_active: bool

    def __post_init__(self) -> None:
        flow4df.tools.schema.assert_columns_in_schema(
            columns=self.partition_spec.columns, schema=self.table_schema
        )

    @functools.cached_property
    def location(self) -> str:
        return self.storage.build_location(self.table_identifier)

    @functools.cached_property
    def column_types(self) -> dict[str, T.DataType]:
        """<column_name>:<column_type> mapping."""
        return {
            e.name: e.dataType for e in self.table_schema.fields
        }

    @functools.cached_property
    def _upstream_tables_dict(self) -> dict[str, Table]:
        return {
            t.table_identifier.name: t for t in self.upstream_tables
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
        assert spark is not None  # filled by the decorator
        return self._as_df(
            reader=spark.read, location=self.location, options=options
        )

    @fill_in_spark_session
    def as_streaming_df(
        self,
        spark: SparkSession | None = None,
        options: dict[str, Any] | None = None,
    ) -> DataFrame:
        assert spark is not None  # filled by the decorator
        return self._as_df(
            reader=spark.readStream, location=self.location, options=options
        )

    def get_upstream_table(self, name: str) -> Table:
        if name not in self._upstream_tables_dict:
            upstream_table_names = self._upstream_tables_dict.keys()
            closest = difflib.get_close_matches(
                word=name, possibilities=upstream_table_names, n=3, cutoff=0
            )
            _m = f'Cannot find table `{name}`. Did you mean one of {closest}?'
            raise ValueError(_m)

        return self._upstream_tables_dict[name]

    @fill_in_spark_session
    def run(
        self,
        spark: SparkSession | None = None,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
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

    def test_transformation(
        self,
        spark: SparkSession,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        self.transformation.test_transformation(
            spark=spark, this_table=self, data_interval=data_interval
        )

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

    @staticmethod
    def find_table_in_module(module: ModuleType) -> Table | None:
        members = inspect.getmembers(
            object=module, predicate=lambda e: isinstance(e, Table)
        )
        if len(members) == 0:
            return None

        _m = f'More than 1 Table instance found in `{module.__name__}`'
        assert len(members) == 1, _m
        return members[0][1]


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
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> StreamingQuery | None:
        ...

    def test_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        ...
