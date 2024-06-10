import re
import logging
import operator
import functools
from typing import TypeAlias, Union, Any
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

from flow4df.table_identifier import TableIdentifier
from flow4df.storage.storage import Storage
from flow4df.storage_backend import StorageBackend
from flow4df.partitioning import Partitioning
from flow4df.data_interval import DataInterval

TABLE_FORMAT = 'delta'
Reader: TypeAlias = Union[DataFrameReader, DataStreamReader]
Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]
log = logging.getLogger()


@dataclass(frozen=True, kw_only=True)
class Constraint:
    name: str
    expression: str


@dataclass(frozen=True, kw_only=True)
class DeltaStorage(Storage):
    table_identifier: TableIdentifier
    storage_backend: StorageBackend
    partitioning: Partitioning
    stateful_query_source: bool
    merge_schema: bool = True
    z_order_by: str | None = None
    constraints: list[Constraint] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._assert_compatibility()

    def build_batch_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        return self._build_df(reader=spark.read, options=options)

    def build_streaming_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        return self._build_df(reader=spark.readStream, options=options)

    def configure_writer(
            self, writer: Writer, data_interval: DataInterval | None = None
    ) -> Writer:
        """Configures the given Writer and returns it.

        Sets:
            - .format('delta')
            - .option('path', '<location>')
            - .option('mergeSchema', True|False)
            For idempotency of batch writes:
            - .option('txnAppId', '<table_id>')
            - .option('txnVersion', <data_interval.start_unix_ts_seconds>)
        """
        writer = (
            writer
            .format(TABLE_FORMAT)
            .option('path', self.location)
            .option('mergeSchema', self.merge_schema)
        )
        table_id = self.table_identifier.table_id
        if data_interval is not None:
            writer = (
                writer
                .option('txnAppId', f'transform_{table_id}')
                .option('txnVersion', data_interval.start_unix_ts_seconds)
            )

        return writer

    def build_checkpoint_location(self, checkpoint_dir: str) -> str:
        cp_location = self.storage_backend.build_checkpoint_location(
            table_identifier=self.table_identifier,
            checkpoint_dir=checkpoint_dir,
            table_suffix=TABLE_FORMAT
        )
        return cp_location

    def run_storage_maintenance(
        self, spark: SparkSession, column_types: dict[str, T.DataType]
    ) -> None:
        optimizer = self._build_delta_table(spark=spark)
        dt = self._build_delta_table(spark=spark)
        if not self.stateful_query_source:
            if self.z_order_by is None:
                dt.optimize().executeCompaction()
            else:
                dt.optimize().executeZOrderBy(self.z_order_by)

            return None

        while True:
            part_to_compact = self.find_partition_to_compact(
                spark=spark, column_types=column_types
            )
            if part_to_compact is None:
                break
            tname = self.table_identifier.name
            log.warning(f'Compacting: {tname}\n{part_to_compact}')
            optimizer = dt.optimize().where(part_to_compact)
            assert self.z_order_by is not None
            optimizer.executeZOrderBy(self.z_order_by)

        return None

    def init_storage(
        self, spark: SparkSession, schema: T.StructType
    ) -> None:
        """TODO: Debug, test with different backends."""
        # Issue when setting file:///tmp/blah as location
        # `CREATE TABLE contains two different locations:`
        # `file:///... vs file:/...`
        # To investigate further, mb a bug? For now just remove it
        location = re.sub('^file://', '', self.location)
        builder = (
            DeltaTable
            .createIfNotExists(sparkSession=spark)
            .location(location)
            .addColumns(schema)
            .partitionedBy(*self.partitioning.columns)
        )
        for c in self.constraints:
            builder = builder.property(
                key=f'delta.constraints.{c.name}', value=c.expression
            )

        builder.execute()
        return None

    def build_storage_stats(self, spark: SparkSession) -> DataFrame:
        raw_log_snapshot = self.build_log_snapshot_df(spark=spark)
        num_rows = F.get_json_object('stats', '$.numRecords').cast(
            T.LongType()
        )
        size_gib = F.sum('size') / 1_073_741_824
        agg_cols = [
            F.sum(num_rows).alias('total_rows'),
            F.count('*').alias('n_files'),
            F.format_number(size_gib, 2).alias('total_size_gib'),
            # last_file_ts.alias('last_file_ts'),
            # since_last_file.alias('since_last_file')
        ]
        return raw_log_snapshot.select(agg_cols)

    @property
    def location(self) -> str:
        return self.storage_backend.build_location(
            table_identifier=self.table_identifier,
            table_suffix=TABLE_FORMAT,
        )

    # End of Storage protocol
    def build_log_snapshot_df(self, spark: SparkSession) -> DataFrame:
        raw_log_snapshot_df = self._build_log_snapshot_df(
            spark=spark, location=self.location
        )
        return raw_log_snapshot_df

    def find_partition_to_compact(
        self, spark: SparkSession, column_types: dict[str, T.DataType]
    ) -> str | None:
        raw_log_snapshot_df = self.build_log_snapshot_df(spark=spark)
        log_snapshot_df = self.add_partitioning_info(
            log_snapshot_df=raw_log_snapshot_df,
            partitioning=self.partitioning,
            column_types=column_types
        )
        preds = [
            F.col('n_files') > F.lit(1),
            F.col('prev_mod_time') > F.col('modificationTime')
        ]
        predicate = functools.reduce(operator.or_, preds)
        _pe = 'part_expression'
        part_to_compact = log_snapshot_df.where(predicate).select(
            F.min_by(_pe, 'part_struct').alias(_pe)
        ).collect()

        unordered_partition = None
        if len(part_to_compact) > 0:
            unordered_partition = part_to_compact[0][_pe]

        return unordered_partition

    def _build_df(
        self, reader: Reader, options: dict[str, Any] | None = None
    ) -> DataFrame:
        location = self.storage_backend.build_location(
            table_identifier=self.table_identifier,
            table_suffix=TABLE_FORMAT
        )
        configured_reader = (
            reader.format(TABLE_FORMAT).option('path', location)
        )
        if options is not None:
            configured_reader = configured_reader.options(**options)
        return configured_reader.load()

    def _assert_compatibility(self) -> None:
        """TODO:"""
        if self.stateful_query_source:
            _m = 'Partitioning must have time_monotonic_increasing columns'
            _tmi = self.partitioning.time_monotonic_increasing
            assert len(_tmi) > 0, _m

            _m1 = 'Must define `z_order_by` for compaction'
            assert self.z_order_by is not None, _m1

    def _build_delta_table(self, spark: SparkSession) -> DeltaTable:
        return DeltaTable.forPath(sparkSession=spark, path=self.location)

    def _inspect_log_df(
        self, spark: SparkSession, column_types: dict[str, T.DataType]
    ) -> DataFrame:
        raw_log_snapshot_df = self.build_log_snapshot_df(spark=spark)
        log_snapshot_df = self.add_partitioning_info(
            log_snapshot_df=raw_log_snapshot_df,
            partitioning=self.partitioning,
            column_types=column_types
        )
        ms_since_previous = F.col('modificationTime') - F.col('prev_mod_time')
        size_mib = F.col('size') / F.lit(1024**2)
        cols = [
            F.col('part_struct'),
            F.col('n_files'),
            F.col('file_added_ts'),
            ms_since_previous.alias('ms_since_previous'),
            F.format_number(size_mib, 2).alias('size_MiB'),
        ]
        return (
            log_snapshot_df
            .select(cols)
            .orderBy('part_struct')
        )

    @staticmethod
    def _build_log_snapshot_df(
        spark: SparkSession, location: str
    ) -> DataFrame:
        j_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog  # type: ignore
        delta_log = j_log.forTable(  # type: ignore
            spark._jsparkSession, location
        )
        jvm_table_files = (
            delta_log.snapshot().allFiles().toDF()  # type: ignore
        )
        log_snapshot_df = DataFrame(jvm_table_files, spark)
        return log_snapshot_df

    @staticmethod
    def add_partitioning_info(
        log_snapshot_df: DataFrame,
        partitioning: Partitioning,
        column_types: dict[str, T.DataType]
    ) -> DataFrame:
        pv_map = F.col('partitionValues')
        pred_map = F.transform_values(
            col=pv_map, f=lambda k, v: F.format_string('%s = "%s"', k, v)
        )
        part_expression = F.array_join(
            F.map_values(pred_map), delimiter=' AND '
        )
        part_struct = F.lit(None)
        if partitioning.n_columns > 0:
            part_struct = F.struct([
                pv_map.getItem(c).cast(column_types[c]).alias(c)
                for c in partitioning.columns
            ])

        non_increasing: list[Column] = [
            part_struct.getField(e.name)
            for e in partitioning.time_non_monotonic
        ]
        w1 = Window.partitionBy(*non_increasing).orderBy(part_struct)
        w2 = Window.partitionBy(part_struct).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df = log_snapshot_df.withColumns({
            'part_expression': part_expression,
            'part_struct': part_struct,
            'prev_mod_time': F.lag('modificationTime').over(w1),
            'n_files': F.count('*').over(w2),
        })
        return df.withColumns({
            'file_added_ts': F.timestamp_millis('modificationTime')
        })
