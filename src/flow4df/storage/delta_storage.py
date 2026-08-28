import re
import logging
import operator
import functools
import textwrap
from typing import TypeAlias, Union, Any
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

from flow4df.table_identifier import TableIdentifier
from flow4df.storage.storage import Storage
from flow4df.storage_backend import StorageBackend
from flow4df.partitioning import Partitioning
from flow4df.data_interval import DataInterval

TABLE_FORMAT = 'delta'
log = logging.getLogger()
Reader: TypeAlias = Union[DataFrameReader, DataStreamReader]
Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]


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
    constraints: list[Constraint] = field(default_factory=list)
    use_catalog: bool = False
    catalog_name: str | None = None

    def __post_init__(self) -> None:
        self._assert_compatibility()
        if self.use_catalog:
            _m = '`catalog_name` must be specified!'
            assert self.catalog_name is not None, _m

    def build_batch_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        return self._build_df(reader=spark.read, options=options)

    def build_streaming_df(
        self, spark: SparkSession, options: dict[str, Any] | None = None
    ) -> DataFrame:
        return self._build_df(reader=spark.readStream, options=options)

    def build_delta_table(self, spark: SparkSession) -> DeltaTable:
        if self.use_catalog:
            dt = DeltaTable.forName(spark, tableOrViewName=self.canonical_name)
        else:
            dt = DeltaTable.forPath(spark, path=self.location)
        return dt

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
            .option('mergeSchema', self.merge_schema)
        )
        if not self.use_catalog:
            writer = writer.option('path', self.location)

        table_id = self.table_identifier.table_id
        if data_interval is not None:
            writer = (
                writer
                .option('txnAppId', f'transform_{table_id}')
                .option('txnVersion', data_interval.start_unix_ts_seconds)
            )

        return writer

    def run_streaming_writer(self, writer: DataStreamWriter) -> StreamingQuery:
        if not self.use_catalog:
            return writer.start()
        else:
            return writer.toTable(self.canonical_name)

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
        dt = self.build_delta_table(spark=spark)
        if not self.stateful_query_source:
            dt.optimize().executeCompaction()
            return None

        node_data_frame = dt.toDF()
        while True:
            part_to_compact = self.find_partition_to_compact(
                spark=spark, column_types=column_types
            )
            if part_to_compact is None:
                break

            tname = self.table_identifier.name
            log.warning(f'Compacting: {tname}\n{part_to_compact}')
            self.compact_partition(
                partition_predicate=part_to_compact,
                node_data_frame=node_data_frame,
            )

        return None

    def init_storage(
        self, spark: SparkSession, schema: T.StructType
    ) -> None:
        """TODO: Debug, test with different backends."""
        if not self.use_catalog:
            # Issue when setting file:///tmp/blah as location
            # `CREATE TABLE contains two different locations:`
            # `file:///... vs file:/...`
            # To investigate further, mb a bug? For now just remove it
            builder = (
                DeltaTable
                .createIfNotExists(sparkSession=spark)
                .addColumns(schema)
                .partitionedBy(*self.partitioning.columns)
            )
            location = re.sub('^file://', '', self.location)
            builder = builder.location(location)
            for c in self.constraints:
                builder = builder.property(
                    key=f'delta.constraints.{c.name}', value=c.expression
                )

            builder.execute()

        elif self.use_catalog:
            constraint_props = [
                f"'delta.constraints.{c.name}' = '{c.expression}'"
                for c in self.constraints
            ]
            props = [
                "'delta.logRetentionDuration' = 'INTERVAL 30 DAYS'"
            ]
            tbl_props = ', '.join(props + constraint_props)
            create_table_q = textwrap.dedent(f"""
            CREATE TABLE IF NOT EXISTS {self.canonical_name} (
              {schema.toDDL()}
            )
            USING DELTA
            PARTITIONED BY ({', '.join(self.partitioning.columns)})
            TBLPROPERTIES ({tbl_props})
            """)
            spark.sql(create_table_q)

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

    @property
    def canonical_name(self) -> str:
        return f'{self.catalog_name}.{self.table_identifier.table_id}'

    # End of Storage protocol
    def build_log_snapshot_df(self, spark: SparkSession) -> DataFrame:
        if self.use_catalog:
            return self._build_metadata_log_snapshot_df(
                node_data_frame=self.build_batch_df(spark),
                partitioning=self.partitioning
            )

        return self._build_log_snapshot_df(spark=spark, location=self.location)

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

    def compact_partition(
        self, partition_predicate: str, node_data_frame: DataFrame,
    ) -> None:
        part_df = node_data_frame.where(partition_predicate).repartition(1)
        writer = (
            part_df.write
            .format(TABLE_FORMAT)
            .mode('overwrite')
            # .option('path', self.location)
            .option('dataChange', False)
            .option('replaceWhere', partition_predicate)
        )
        if self.use_catalog:
            writer.saveAsTable(name=self.canonical_name)
        else:
            writer.save(path=self.location)
        return None

    def _build_df(
        self, reader: Reader, options: dict[str, Any] | None = None
    ) -> DataFrame:
        configured_reader = reader.format(TABLE_FORMAT)
        if options is not None:
            configured_reader = configured_reader.options(**options)

        if self.use_catalog:
            return configured_reader.table(self.canonical_name)

        location = self.storage_backend.build_location(
            table_identifier=self.table_identifier,
            table_suffix=TABLE_FORMAT
        )
        return configured_reader.load(path=location)

    def _assert_compatibility(self) -> None:
        """TODO:"""
        if self.stateful_query_source:
            _m = 'Partitioning must have time_monotonic_increasing columns'
            _tmi = self.partitioning.time_monotonic_increasing
            assert len(_tmi) > 0, _m

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
        j_logs = [
            # OSS Delta
            spark._jvm.org.apache.spark.sql.delta.DeltaLog,  # type: ignore
            # Databricks Delta
            spark._jvm.com.databricks.sql.transaction.tahoe.DeltaLog  # type: ignore
        ]
        for j_log in j_logs:
            try:
                delta_log = j_log.forTable( # type: ignore
                    spark._jsparkSession, location
                )
                if delta_log is not None:
                    break
            except Exception:
                delta_log = None

        _m = 'Cannot build DeltaLog snapshot!'
        assert delta_log is not None, _m  # type: ignore
        jvm_table_files = (
            delta_log.snapshot().allFiles().toDF()  # type: ignore
        )
        log_snapshot_df = DataFrame(jvm_table_files, spark)
        return log_snapshot_df

    @staticmethod
    def _build_metadata_log_snapshot_df(
        node_data_frame: DataFrame,
        partitioning: Partitioning,
    ) -> DataFrame:

        def n_add_partition_values(
            snapshot_df: DataFrame, partitioning_columns: list[str]
        ) -> DataFrame:
            fp = F.col('file_path')
            parsed = F.str_to_map(
                fp, pairDelim=F.lit('/'), keyValueDelim=F.lit('=')
            )
            part_values = F.map_filter(
                parsed, lambda k, _: k.isin(*partitioning_columns)
            )
            return snapshot_df.withColumn('partitionValues', part_values)

        def n_add_modtime_millis(snapshot_df: DataFrame) -> DataFrame:
            mod_time_millis = F.unix_millis('file_modification_time')
            return snapshot_df.withColumn('modificationTime', mod_time_millis)

        md = F.col('_metadata')
        mod_time = md.getField('file_modification_time')
        cols = [
            mod_time.alias('file_modification_time'),
            md.getField('file_path').alias('file_path'),
            md.getField('file_name').alias('file_name')
        ]
        return (
            node_data_frame
            .select(cols)
            .groupBy('file_modification_time', 'file_path', 'file_name')
            .agg(F.count('*').alias('row_count'))
            .transform(
                n_add_partition_values,
                partitioning_columns=partitioning.columns
            )
            .transform(n_add_modtime_millis)
        )

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
