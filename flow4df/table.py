from __future__ import annotations
import inspect
import difflib
import logging
import unittest
import functools
import datetime as dt
from collections import Counter
from abc import abstractmethod
from types import ModuleType
from typing import Any, Protocol, Callable, TypeVar, ParamSpec, NoReturn
from typing import TYPE_CHECKING
from dataclasses import dataclass, field, replace
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming.readwriter import DataStreamWriter
from pyspark.sql.types import StructType, DataType

if TYPE_CHECKING:
    import duckdb  # type: ignore

import flow4df
from flow4df import type_annotations, enums
from flow4df.table_format.table_format import TableFormat
from flow4df.storage.storage import Storage
from flow4df.table_stats import TableStats
from flow4df.column_stats import ColumnStats

T = TypeVar('T')
P = ParamSpec('P')
log = logging.getLogger(__name__)
integration_tests_default = field(default_factory=list, repr=False)


def fill_in_spark_session(func: Callable[P, T]) -> Callable[P, T]:

    @functools.wraps(func)
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
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


class IntegrationTest(Protocol):
    def __call__(
        self, spark: SparkSession, table: Table,
    ) -> None:
        ...


@dataclass(frozen=False, kw_only=True)
class Table:
    table_schema: StructType = field(repr=False)
    table_identifier: flow4df.TableIdentifier
    upstream_table_identifiers: list[flow4df.TableIdentifier]
    table_index: TableIndex | None
    transformation: Transformation
    table_format: TableFormat
    storage: Storage
    storage_stub: Storage
    partition_spec: flow4df.PartitionSpec
    is_active: bool
    integration_tests: list[IntegrationTest] = integration_tests_default

    def __post_init__(self) -> None:
        flow4df.tools.schema.assert_columns_in_schema(
            columns=self.partition_spec.partition_columns,
            schema=self.table_schema
        )
        if len(self.upstream_table_identifiers) > 0:
            _m = 'Table with upstream table dependencies must have TableIndex!'
            assert self.table_index is not None, _m

    @functools.cached_property
    def location(self) -> str:
        return self.storage.build_location(self.table_identifier)

    @functools.cached_property
    def column_types(self) -> dict[str, DataType]:
        """<column_name>:<column_type> mapping."""
        return {
            e.name: e.dataType for e in self.table_schema.fields
        }

    @functools.cached_property
    def upstream_tables(self) -> list[Table]:
        if self.table_index is None:
            return []

        tables_ = []
        for ti in self.upstream_table_identifiers:
            if ti.version is None:
                table = self.table_index.get_active_table(
                    catalog=ti.catalog, schema=ti.schema, name=ti.name
                )
            else:
                table = self.table_index.get_table(
                    catalog=ti.catalog, schema=ti.schema, name=ti.name,
                    version=ti.version
                )

            tables_.append(table)

        return tables_

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
            table_identifier=self.table_identifier,
            table_schema=self.table_schema,
            partition_spec=self.partition_spec
        )

    def init_table_stub(self, spark: SparkSession) -> None:
        """Initizalize the Table stub, used for unit testing."""
        stub_location = self.storage_stub.build_location(self.table_identifier)
        self.table_format.init_table(
            spark=spark,
            location=stub_location,
            table_identifier=self.table_identifier,
            table_schema=self.table_schema,
            partition_spec=self.partition_spec
        )

    def _as_df(
        self,
        reader: type_annotations.Reader,
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
        stub_table_index: TableIndex,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        unit_test_table = replace(
            self, table_index=stub_table_index, storage=self.storage_stub
        )
        self.transformation.test_transformation(
            spark=spark,
            unit_test_table=unit_test_table,
            data_interval=data_interval
        )

    @fill_in_spark_session
    def get_column_stats(
        self,
        column_name: str,
        spark: SparkSession | None = None,
    ) -> ColumnStats:
        assert spark is not None
        return self.table_format.get_column_stats(
            spark=spark,
            location=self.location,
            column_types=self.column_types,
            column_name=column_name,
            table_identifier=self.table_identifier
        )

    @fill_in_spark_session
    def is_initialized_only(self, spark: SparkSession | None = None) -> bool:
        assert spark is not None
        return self.table_format.is_initialized_only(
            spark=spark,
            location=self.location,
            table_identifier=self.table_identifier
        )

    def calculate_table_stats(self, spark: SparkSession) -> TableStats:
        return self.table_format.calculate_table_stats(spark, self.location)

    @staticmethod
    def find_table_in_module(module: ModuleType) -> Table | None:
        inferred_identifier = flow4df.TableIdentifier.from_module_name(
            module_name=module.__name__
        )

        def is_matching(member: Any) -> bool:
            is_table = isinstance(member, Table)
            if not is_table:
                return False

            member_identifier: flow4df.TableIdentifier = getattr(
                member, 'table_identifier'
            )
            return member_identifier.is_semantically_equivalent(
                catalog=inferred_identifier.catalog,
                schema=inferred_identifier.schema,
                name=inferred_identifier.name
            )

        members = inspect.getmembers(object=module, predicate=is_matching)
        if len(members) == 0:
            return None

        _m = f'More than 1 Table instance found in `{module.__name__}`'
        assert len(members) < 2, _m
        return members[0][1]

    def run_integration_tests(
        self, spark: SparkSession
    ) -> unittest.TestResult:
        test_functions = {
            f.__name__: functools.partial(  # type: ignore
                f, spark=spark, table=self
            )
            for f in self.integration_tests
        }

        class TestIntegration(unittest.TestCase):
            def test_integration_tests(self):
                for fname, tf in test_functions.items():
                    with self.subTest(msg=f'Function: {fname}'):
                        tf()

        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(testCaseClass=TestIntegration)
        runner = unittest.TextTestRunner()
        return runner.run(suite)

    @fill_in_spark_session
    def print_schema(self, spark: SparkSession) -> None:
        """Use DataFrame.printSchema() to print the Table schema."""
        empty_df = spark.createDataFrame(data=[], schema=self.table_schema)
        empty_df.printSchema()

    def as_duckdb_rel(self) -> duckdb.DuckDBPyRelation:
        import duckdb
        loc = self.storage.build_canonical_location(self.table_identifier)
        query = self.table_format.build_duckdb_query(canonical_location=loc)
        return duckdb.sql(query)


class Transformation(Protocol):

    @abstractmethod
    def run_transformation(
        self,
        spark: SparkSession,
        this_table: Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> StreamingQuery | None:
        ...

    @abstractmethod
    def test_transformation(
        self,
        spark: SparkSession,
        unit_test_table: Table,
        trigger: flow4df.Trigger | None = None,
        data_interval: flow4df.DataInterval | None = None,
    ) -> None:
        ...

    def build_next_data_interval(
        self, spark: SparkSession, this_table: Table,
    ) -> flow4df.DataInterval | None:
        raise

    def start_writer(
        self, writer: type_annotations.Writer, output_mode: enums.OutputMode
    ) -> StreamingQuery | None:
        if isinstance(writer, DataFrameWriter):
            return writer.save()
        elif isinstance(writer, DataStreamWriter):
            return writer.start()
        elif isinstance(writer, DataFrameWriterV2):
            if output_mode == enums.OutputMode.append:
                return writer.append()
            elif output_mode == enums.OutputMode.overwrite:
                return writer.overwrite(F.lit(True))


@dataclass(frozen=True, kw_only=True)
class TableIndex:
    """TODO doc."""
    package: str
    stub: bool = False

    @staticmethod
    def assert_single_active_table(active_tables: list[flow4df.Table]) -> None:
        versionless_names = [
            t.table_identifier.versionless_name for t in active_tables
        ]
        counter = Counter(versionless_names)
        with_multiple_versions = [
            name for name, count in counter.items() if count > 1
        ]
        _m = f'Multiple versions for: {with_multiple_versions} !'
        assert len(with_multiple_versions) == 0, _m
        return None

    @property  # Doesn't work if cached!
    def all_tables(self) -> list[flow4df.Table]:
        modules = flow4df.tools.module.list_modules(root_package=self.package)
        attempted_tables = [
            flow4df.Table.find_table_in_module(m) for m in modules
        ]
        tables = [t for t in attempted_tables if t is not None]
        if self.stub:
            tables = [
                replace(t, storage=t.storage_stub) for t in tables
            ]

        return tables

    @staticmethod
    def raise_no_matching(
        identifier: tuple[str, ...], tables: list[flow4df.Table]
    ) -> NoReturn:
        possibilities = [
            t.table_identifier.versionless_name for t in tables
        ]
        table_name = '.'.join(identifier)
        closest = difflib.get_close_matches(
            word=table_name, possibilities=possibilities, n=3, cutoff=0
        )
        _m = (
            f'Cannot find table `{table_name}`. '
            f'Did you mean one of {closest}?'
        )
        raise ValueError(_m)

    @functools.cache
    def get_active_table(
        self, catalog: str, schema: str, name: str
    ) -> Table | NoReturn:
        active_tables = [t for t in self.all_tables if t.is_active]
        TableIndex.assert_single_active_table(active_tables)
        for at in active_tables:
            is_same = at.table_identifier.is_semantically_equivalent(
                catalog=catalog, schema=schema, name=name,
            )
            if is_same:
                return at

        TableIndex.raise_no_matching(
            identifier=(catalog, schema, name),
            tables=active_tables
        )

    @functools.cache
    def get_table(
        self, catalog: str, schema: str, name: str, version: str
    ) -> Table | NoReturn:
        all_tables = self.all_tables
        requested_identifier = flow4df.TableIdentifier(
            catalog=catalog, schema=schema, name=name, version=version
        )
        for table in self.all_tables:
            if table.table_identifier == requested_identifier:
                return table

        TableIndex.raise_no_matching(
            identifier=(catalog, schema, name, version),
            tables=all_tables
        )

